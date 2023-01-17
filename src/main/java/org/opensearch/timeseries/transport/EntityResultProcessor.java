package org.opensearch.timeseries.transport;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdEntityWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.ratelimit.ResultWriteWorker;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Shared code to implement an entity result transportation
 * (e.g., EntityForecastResultTransportAction)
 *
 */
public class EntityResultProcessor<RCFModelType extends ThresholdedRandomCutForest, IntermediateResultType extends IntermediateResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, IndexableResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<IndexableResultType>, ResultWriteBatchRequestType extends ResultBulkRequest<IndexableResultType, ResultWriteRequestType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ModelColdStartType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType>, ModelManagerType extends ModelManager<RCFModelType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType>, CacheType extends TimeSeriesCache<RCFModelType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, CacheType>, ResultHandlerType extends IndexMemoryPressureAwareResultHandler<ResultWriteBatchRequestType, ResultBulkResponse, IndexType, IndexManagementType>, ResultWriteWorkerType extends ResultWriteWorker<IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, IndexType, IndexManagementType, ResultHandlerType>, HCCheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, CacheType, ColdStartWorkerType, ResultHandlerType, ResultWriteWorkerType>, ColdEntityWorkerType extends ColdEntityWorker<RCFModelType, IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, IndexType, IndexManagementType, CheckpointDaoType, IntermediateResultType, ModelManagerType, CheckpointWriteWorkerType, ModelColdStartType, CacheType, ColdStartWorkerType, ResultHandlerType, ResultWriteWorkerType, HCCheckpointReadWorkerType>> {

    private static final Logger LOG = LogManager.getLogger(EntityResultProcessor.class);

    private CacheProvider<RCFModelType, CacheType> cache;
    private ModelManagerType modelManager;
    private IndexType resultIndex;
    private IndexManagementType indexUtil;
    private ResultWriteWorkerType resultWriteQueue;
    private Class<ResultWriteRequestType> resultWriteClass;
    private Stats stats;
    private ColdStartWorkerType entityColdStartWorker;
    private HCCheckpointReadWorkerType checkpointReadQueue;
    private ColdEntityWorkerType coldEntityQueue;

    public EntityResultProcessor(
        CacheProvider<RCFModelType, CacheType> cache,
        ModelManagerType manager,
        IndexType resultIndex,
        IndexManagementType indexUtil,
        ResultWriteWorkerType resultWriteQueue,
        Class<ResultWriteRequestType> resultWriteClass,
        Stats stats,
        ColdStartWorkerType entityColdStartWorker,
        HCCheckpointReadWorkerType checkpointReadQueue,
        ColdEntityWorkerType coldEntityQueue
    ) {
        this.cache = cache;
        this.modelManager = manager;
        this.resultIndex = resultIndex;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.resultWriteClass = resultWriteClass;
        this.stats = stats;
        this.entityColdStartWorker = entityColdStartWorker;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
    }

    public ActionListener<Optional<? extends Config>> onGetConfig(
        ActionListener<AcknowledgedResponse> listener,
        String forecasterId,
        EntityResultRequest request,
        Optional<Exception> prevException
    ) {
        return ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(forecasterId, "Config " + forecasterId + " is not available.", false));
                return;
            }

            Config config = configOptional.get();

            if (request.getEntities() == null) {
                listener.onFailure(new EndRunException(forecasterId, "Fail to get any entities from request.", false));
                return;
            }

            Instant executionStartTime = Instant.now();
            Map<Entity, double[]> cacheMissEntities = new HashMap<>();
            for (Entry<Entity, double[]> entityEntry : request.getEntities().entrySet()) {
                Entity entity = entityEntry.getKey();

                if (isEntityFromOldNodeMsg(entity) && config.getCategoryFields() != null && config.getCategoryFields().size() == 1) {
                    Map<String, String> attrValues = entity.getAttributes();
                    // handle a request from a version before OpenSearch 1.1.
                    entity = Entity.createSingleAttributeEntity(config.getCategoryFields().get(0), attrValues.get(CommonName.EMPTY_FIELD));
                }

                Optional<String> modelIdOptional = entity.getModelId(forecasterId);
                if (false == modelIdOptional.isPresent()) {
                    continue;
                }

                String modelId = modelIdOptional.get();
                double[] datapoint = entityEntry.getValue();
                ModelState<RCFModelType> entityModel = cache.get().get(modelId, config);
                if (entityModel == null) {
                    // cache miss
                    cacheMissEntities.put(entity, datapoint);
                    continue;
                }
                try {
                    IntermediateResultType result = modelManager
                        .getResult(
                            new Sample(datapoint, Instant.ofEpochMilli(request.getStart()), Instant.ofEpochMilli(request.getEnd())),
                            entityModel,
                            modelId,
                            Optional.ofNullable(entity),
                            config
                        );
                    // result.getRcfScore() = 0 means the model is not initialized
                    // result.getGrade() = 0 means it is not an anomaly
                    // So many OpenSearchRejectedExecutionException if we write no matter what
                    if (result.getRcfScore() > 0) {
                        IndexableResult resultToSave = result
                            .toIndexableResult(
                                config,
                                Instant.ofEpochMilli(request.getStart()),
                                Instant.ofEpochMilli(request.getEnd()),
                                executionStartTime,
                                Instant.now(),
                                ParseUtils.getFeatureData(datapoint, config),
                                Optional.ofNullable(entity),
                                indexUtil.getSchemaVersion(resultIndex),
                                modelId,
                                null,
                                null
                            );

                        ResultWriteRequestType indexableResult = ResultWriteRequest
                            .create(
                                System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                forecasterId,
                                RequestPriority.MEDIUM,
                                resultToSave,
                                config.getCustomResultIndex(),
                                resultWriteClass
                            );

                        resultWriteQueue.put(indexableResult);
                    }
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
                    stats.getStat(StatNames.AD_MODEL_CORRUTPION_COUNT.getName()).increment();
                    cache.get().removeModel(forecasterId, modelId);
                    entityColdStartWorker
                        .put(
                            new FeatureRequest(
                                System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                forecasterId,
                                RequestPriority.MEDIUM,
                                datapoint,
                                request.getStart(),
                                entity
                            )
                        );
                }
            }

            // split hot and cold entities
            Pair<List<Entity>, List<Entity>> hotColdEntities = cache
                .get()
                .selectUpdateCandidate(cacheMissEntities.keySet(), forecasterId, config);

            List<FeatureRequest> hotEntityRequests = new ArrayList<>();
            List<FeatureRequest> coldEntityRequests = new ArrayList<>();

            for (Entity hotEntity : hotColdEntities.getLeft()) {
                double[] hotEntityValue = cacheMissEntities.get(hotEntity);
                if (hotEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", hotEntity));
                    continue;
                }
                hotEntityRequests
                    .add(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                            forecasterId,
                            // hot entities has MEDIUM priority
                            RequestPriority.MEDIUM,
                            hotEntityValue,
                            request.getStart(),
                            hotEntity
                        )
                    );
            }

            for (Entity coldEntity : hotColdEntities.getRight()) {
                double[] coldEntityValue = cacheMissEntities.get(coldEntity);
                if (coldEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", coldEntity));
                    continue;
                }
                coldEntityRequests
                    .add(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                            forecasterId,
                            // cold entities has LOW priority
                            RequestPriority.LOW,
                            coldEntityValue,
                            request.getStart(),
                            coldEntity
                        )
                    );
            }

            checkpointReadQueue.putAll(hotEntityRequests);
            coldEntityQueue.putAll(coldEntityRequests);

            // respond back
            if (prevException.isPresent()) {
                listener.onFailure(prevException.get());
            } else {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        }, exception -> {
            LOG
                .error(
                    new ParameterizedMessage(
                        "fail to get entity's anomaly grade for detector [{}]: start: [{}], end: [{}]",
                        forecasterId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        });
    }

    /**
     * Whether the received entity comes from an node that doesn't support multi-category fields.
     * This can happen during rolling-upgrade or blue/green deployment.
     *
     * Specifically, when receiving an EntityResultRequest from an incompatible node,
     * EntityResultRequest(StreamInput in) gets an String that represents an entity.
     * But Entity class requires both an category field name and value. Since we
     * don't have access to detector config in EntityResultRequest(StreamInput in),
     * we put CommonName.EMPTY_FIELD as the placeholder.  In this method,
     * we use the same CommonName.EMPTY_FIELD to check if the deserialized entity
     * comes from an incompatible node.  If it is, we will add the field name back
     * as EntityResultTranportAction has access to the detector config object.
     *
     * @param categoricalValues deserialized Entity from inbound message.
     * @return Whether the received entity comes from an node that doesn't support multi-category fields.
     */
    private boolean isEntityFromOldNodeMsg(Entity categoricalValues) {
        Map<String, String> attrValues = categoricalValues.getAttributes();
        return (attrValues != null && attrValues.containsKey(CommonName.EMPTY_FIELD));
    }
}
