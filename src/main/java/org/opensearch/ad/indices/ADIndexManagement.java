/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.indices;

import static org.opensearch.ad.constant.ADCommonName.DUMMY_AD_RESULT_ID;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_RESULTS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_PRIMARY_SHARDS;

import java.io.IOException;
import java.net.URL;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * This class provides utility methods for various anomaly detection indices.
 */
public class ADIndexManagement extends IndexManagement<ADIndex> {
    private static final Logger logger = LogManager.getLogger(ADIndexManagement.class);

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;
    private volatile TimeValue historyRetentionPeriod;

    private Scheduler.Cancellable scheduledRollover = null;

    private int maxPrimaryShards;
    // keep track of whether the mapping version is up-to-date
    // private EnumMap<ADIndex, IndexState> indexStates;

    // result index mapping to valida custom index
    private Map<String, Object> AD_RESULT_FIELD_CONFIGS;

    /**
     * Constructor function
     *
     * @param client         OS client supports administrative actions
     * @param clusterService OS cluster service
     * @param threadPool     OS thread pool
     * @param settings       OS cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host AD indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     */
    public ADIndexManagement(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes
    ) {
        super(client, clusterService, threadPool, settings, nodeFilter, maxUpdateRunningTimes, ADIndex.class);
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings);
        this.historyRetentionPeriod = AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.maxPrimaryShards = MAX_PRIMARY_SHARDS.get(settings);

        this.indexStates = new EnumMap<ADIndex, IndexState>(ADIndex.class);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(AD_RESULT_HISTORY_RETENTION_PERIOD, it -> { historyRetentionPeriod = it; });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);

        this.AD_RESULT_FIELD_CONFIGS = null;
    }

    private void initResultMapping() throws IOException {
        if (AD_RESULT_FIELD_CONFIGS != null) {
            // we have already initiated the field
            return;
        }
        String resultMapping = getAnomalyResultMappings();

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
        Object properties = asMap.get(CommonName.PROPERTIES);
        if (properties instanceof Map) {
            AD_RESULT_FIELD_CONFIGS = (Map<String, Object>) properties;
        } else {
            logger.error("Fail to read result mapping file.");
        }
    }

    /**
     * Get anomaly result index mapping json content.
     *
     * @return anomaly result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyResultMappings() throws IOException {
        URL url = ADIndexManagement.class.getClassLoader().getResource(ANOMALY_RESULTS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector state index mapping json content.
     *
     * @return anomaly detector state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getDetectionStateMappings() throws IOException {
        URL url = ADIndexManagement.class.getClassLoader().getResource(ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE);
        String detectionStateMappings = Resources.toString(url, Charsets.UTF_8);
        String detectorIndexMappings = getConfigMappings();
        detectorIndexMappings = detectorIndexMappings
            .substring(detectorIndexMappings.indexOf("\"properties\""), detectorIndexMappings.lastIndexOf("}"));
        return detectionStateMappings.replace("DETECTOR_INDEX_MAPPING_PLACE_HOLDER", detectorIndexMappings);
    }

    /**
     * Get checkpoint index mapping json content.
     *
     * @return checkpoint index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getCheckpointMappings() throws IOException {
        URL url = ADIndexManagement.class.getClassLoader().getResource(CHECKPOINT_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * anomaly result index exist or not.
     *
     * @return true if anomaly result index exists
     */
    @Override
    public boolean doesDefaultResultIndexExist() {
        return clusterService.state().metadata().hasAlias(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    @Override
    public boolean doesIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }

    @Override
    public <T> void initCustomResultIndexAndExecute(String resultIndex, ExecutorFunction function, ActionListener<T> listener) {
        try {
            if (!doesIndexExist(resultIndex)) {
                initCustomAnomalyResultIndexDirectly(resultIndex, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        logger.info("Successfully created anomaly detector result index {}", resultIndex);
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        String error = "Creating anomaly detector result index with mappings call not acknowledged: " + resultIndex;
                        logger.error(error);
                        listener.onFailure(new EndRunException(error, true));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        logger.error("Failed to create anomaly detector result index " + resultIndex, exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                validateCustomResultIndexAndExecute(resultIndex, function, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to create custom result index " + resultIndex, e);
            listener.onFailure(e);
        }
    }

    public <T> void validateCustomIndexForBackendJob(
        String resultIndex,
        String securityLogId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        if (!doesIndexExist(resultIndex)) {
            listener.onFailure(new EndRunException(CommonMessages.CAN_NOT_FIND_RESULT_INDEX + resultIndex, true));
            return;
        }
        if (!isValidResultIndexMapping(resultIndex)) {
            listener.onFailure(new EndRunException("Result index mapping is not correct", true));
            return;
        }
        try (InjectSecurity injectSecurity = new InjectSecurity(securityLogId, settings, client.threadPool().getThreadContext())) {
            injectSecurity.inject(user, roles);
            ActionListener<T> wrappedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
                injectSecurity.close();
                listener.onFailure(e);
            });
            validateCustomResultIndexAndExecute(resultIndex, () -> {
                injectSecurity.close();
                function.execute();
            }, wrappedListener);
        } catch (Exception e) {
            logger.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }

    /**
     * Check if custom result index has correct index mapping.
     * @param resultIndex result index
     * @return true if result index mapping is valid
     */
    @Override
    public boolean isValidResultIndexMapping(String resultIndex) {
        try {
            initResultMapping();
            if (AD_RESULT_FIELD_CONFIGS == null) {
                // failed to populate the field
                return false;
            }
            IndexMetadata indexMetadata = clusterService.state().metadata().index(resultIndex);
            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            String propertyName = CommonName.PROPERTIES;
            if (!indexMapping.containsKey(propertyName) || !(indexMapping.get(propertyName) instanceof LinkedHashMap)) {
                return false;
            }
            LinkedHashMap<String, Object> mapping = (LinkedHashMap<String, Object>) indexMapping.get(propertyName);

            boolean correctResultIndexMapping = true;

            for (String fieldName : AD_RESULT_FIELD_CONFIGS.keySet()) {
                Object defaultSchema = AD_RESULT_FIELD_CONFIGS.get(fieldName);
                // the field might be a map or map of map
                // example: map: {type=date, format=strict_date_time||epoch_millis}
                // map of map: {type=nested, properties={likelihood={type=double}, value_list={type=nested, properties={data={type=double},
                // feature_id={type=keyword}}}}}
                // if it is a map of map, Object.equals can compare them regardless of order
                if (!mapping.containsKey(fieldName) || !defaultSchema.equals(mapping.get(fieldName))) {
                    correctResultIndexMapping = false;
                    break;
                }
            }
            return correctResultIndexMapping;
        } catch (Exception e) {
            logger.error("Failed to validate result index mapping for index " + resultIndex, e);
            return false;
        }

    }

    /**
     * Anomaly state index exist or not.
     *
     * @return true if anomaly state index exists
     */
    public boolean doesDetectorStateIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(ADCommonName.DETECTION_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    @Override
    public boolean doesCheckpointIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(ADCommonName.CHECKPOINT_INDEX_NAME);
    }

    private ActionListener<CreateIndexResponse> markMappingUpToDate(ADIndex index, ActionListener<CreateIndexResponse> followingListener) {
        return ActionListener.wrap(createdResponse -> {
            if (createdResponse.isAcknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping()));
                if (Boolean.FALSE.equals(indexStatetate.mappingUpToDate)) {
                    indexStatetate.mappingUpToDate = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
            followingListener.onResponse(createdResponse);
        }, exception -> followingListener.onFailure(exception));
    }

    /**
     * Create anomaly result index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link ADIndexManagement#getAnomalyResultMappings}
     */
    public void initDefaultAnomalyResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesDefaultResultIndexExist()) {
            initDefaultResultIndexDirectly(actionListener);
        }
    }

    @Override
    protected int getNumberOfPrimaryShards() {
        return Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards);
    }

    /**
     * Create anomaly result index without checking exist or not.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link ADIndexManagement#getAnomalyResultMappings}
     */
    @Override
    public void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        initAnomalyResultIndexDirectly(AD_RESULT_HISTORY_INDEX_PATTERN, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, true, actionListener);
    }

    public void initCustomAnomalyResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener)
        throws IOException {
        initAnomalyResultIndexDirectly(resultIndex, null, false, actionListener);
    }

    public void initAnomalyResultIndexDirectly(
        String resultIndex,
        String alias,
        boolean hiddenIndex,
        ActionListener<CreateIndexResponse> actionListener
    ) throws IOException {
        String mapping = getAnomalyResultMappings();
        CreateIndexRequest request = new CreateIndexRequest(resultIndex).mapping(mapping, XContentType.JSON);
        if (alias != null) {
            request.alias(new Alias(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS));
        }
        choosePrimaryShards(request, hiddenIndex);
        if (AD_RESULT_HISTORY_INDEX_PATTERN.equals(resultIndex)) {
            adminClient.indices().create(request, markMappingUpToDate(ADIndex.RESULT, actionListener));
        } else {
            adminClient.indices().create(request, actionListener);
        }
    }

    /**
     * Create the state index.
     *
     * @param actionListener action called after create index
     */
    public void initDetectionStateIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(ADCommonName.DETECTION_STATE_INDEX)
                .mapping(getDetectionStateMappings(), XContentType.JSON)
                .settings(settings);
            adminClient.indices().create(request, markMappingUpToDate(ADIndex.STATE, actionListener));
        } catch (IOException e) {
            logger.error("Fail to init AD detection state index", e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Create the checkpoint index.
     *
     * @param actionListener action called after create index
     * @throws EndRunException EndRunException due to failure to get mapping
     */
    @Override
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        CreateIndexRequest request = new CreateIndexRequest(ADCommonName.CHECKPOINT_INDEX_NAME).mapping(mapping, XContentType.JSON);
        choosePrimaryShards(request, true);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CHECKPOINT, actionListener));
    }

    @Override
    public void onClusterManager() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAndDeleteHistoryIndex();

            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        } catch (Exception e) {
            // This should be run on cluster startup
            logger.error("Error rollover AD result indices. " + "Can't rollover AD result until clusterManager node is restarted.", e);
        }
    }

    @Override
    public void offClusterManager() {
        if (scheduledRollover != null) {
            scheduledRollover.cancel();
        }
    }

    private String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    private void rescheduleRollover() {
        if (clusterService.state().getNodes().isLocalNodeElectedClusterManager()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    void rolloverAndDeleteHistoryIndex() {
        if (!doesDefaultResultIndexExist()) {
            return;
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        RolloverRequest rollOverRequest = new RolloverRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, null);
        String adResultMapping = null;
        try {
            adResultMapping = getAnomalyResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over AD result index, as can't get AD result index mapping");
            return;
        }
        CreateIndexRequest createRequest = rollOverRequest.getCreateIndexRequest();

        createRequest.index(AD_RESULT_HISTORY_INDEX_PATTERN).mapping(adResultMapping, XContentType.JSON);

        choosePrimaryShards(createRequest, true);

        rollOverRequest.addMaxIndexDocsCondition(historyMaxDocs * getNumberOfPrimaryShards());
        adminClient.indices().rolloverIndex(rollOverRequest, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger
                    .warn(
                        "{} not rolled over. Conditions were: {}",
                        ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
                        response.getConditionStatus()
                    );
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(ADIndex.RESULT, k -> new IndexState(k.getMapping()));
                indexStatetate.mappingUpToDate = true;
                logger.info("{} rolled over. Conditions were: {}", ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
                deleteOldHistoryIndices(ALL_AD_RESULTS_INDEX_PATTERN, historyRetentionPeriod);
            }
        }, exception -> { logger.error("Fail to roll over result index", exception); }));
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link ADIndexManagement#getAnomalyDetectorMappings}
     */
    @Override
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        super.initConfigIndex(markMappingUpToDate(ADIndex.CONFIG, actionListener));
    }

    /**
     * Create job index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        super.initJobIndex(markMappingUpToDate(ADIndex.JOB, actionListener));
    }

    @Override
    protected IndexRequest createDummyIndexRequest(String resultIndex) throws IOException {
        AnomalyResult dummyResult = AnomalyResult.getDummyResult();
        return new IndexRequest(resultIndex)
            .id(DUMMY_AD_RESULT_ID)
            .source(dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
    }

    @Override
    protected DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException {
        return new DeleteRequest(resultIndex).id(DUMMY_AD_RESULT_ID);
    }
}
