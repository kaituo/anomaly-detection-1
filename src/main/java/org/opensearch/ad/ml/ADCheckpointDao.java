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

package org.opensearch.ad.ml;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.client.Client;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;

/**
 * DAO for model checkpoints.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ADCheckpointDao extends CheckpointDao<ThresholdedRandomCutForest, ADIndex, ADDelegatingDataManagement, ADCheckpointCodec>
    implements
        ADCheckpointStore {

    private static final Logger logger = LogManager.getLogger(ADCheckpointDao.class);

    private final Class<? extends ThresholdingModel> thresholdingModelClass;

    private final Clock clock;
    private final Gson gson;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param gson accessor to Gson functionality
     * @param mapper RCF model serialization utility
     * @param converter converter from rcf v1 serde to protostuff based format
     * @param trcfMapper TRCF serialization mapper
     * @param trcfSchema TRCF serialization schema
     * @param thresholdingModelClass thresholding model's class
     * @param indexUtil Index utility methods
     * @param maxCheckpointBytes max checkpoint size in bytes
     * @param serializeRCFBufferPool object pool for serializing rcf models
     * @param serializeRCFBufferSize the size of the buffer for RCF serialization
     * @param anomalyRate anomaly rate
     */
    public ADCheckpointDao(
        Client client,
        ClientUtil clientUtil,
        Gson gson,
        RandomCutForestMapper mapper,
        V1JsonToV3StateConverter converter,
        ThresholdedRandomCutForestMapper trcfMapper,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        ADDelegatingDataManagement dataManagement,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        double anomalyRate,
        Clock clock
    ) {
        super(
            client,
            clientUtil,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            dataManagement,
            new ADCheckpointCodec(
                maxCheckpointBytes,
                trcfSchema,
                trcfMapper,
                converter,
                gson,
                mapper,
                thresholdingModelClass,
                anomalyRate,
                clock,
                serializeRCFBufferPool,
                serializeRCFBufferSize,
                dataManagement
            )
        );
        this.thresholdingModelClass = thresholdingModelClass;
        this.clock = clock;
        this.gson = gson;
    }

    /**
     * Puts a rcf model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param forest the rcf model
     * @param listener onResponse is called with null when the operation is completed
     */
    @Override
    public void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        String modelCheckpoint = checkpointCodec.toCheckpoint(forest);
        if (modelCheckpoint != null) {
            source.put(CommonName.FIELD_MODELV2, modelCheckpoint);
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
            putModelCheckpoint(modelId, source, listener);
        } else {
            listener.onFailure(new RuntimeException("Fail to create checkpoint to save"));
        }
    }

    /**
     * Puts a thresholding model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param threshold the thresholding model
     * @param listener onResponse is called with null when the operation is completed
     */
    @Override
    public void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener) {
        String modelCheckpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> gson.toJson(threshold));
        Map<String, Object> source = new HashMap<>();
        source.put(CommonName.FIELD_MODEL, modelCheckpoint);
        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        putModelCheckpoint(modelId, source, listener);
    }

    private void deserializeTRCFModel(
        GetResponse response,
        String rcfModelId,
        ActionListener<Optional<ThresholdedRandomCutForest>> listener
    ) {
        Object model = null;
        if (response.isExists()) {
            try {
                model = response.getSource().get(CommonName.FIELD_MODELV2);
                if (model != null) {
                    listener.onResponse(Optional.ofNullable(checkpointCodec.toTrcf((String) model)));
                } else {
                    Object modelV1 = response.getSource().get(CommonName.FIELD_MODEL);
                    Optional<RandomCutForest> forest = checkpointCodec.deserializeRCFModel((String) modelV1, rcfModelId);
                    if (!forest.isPresent()) {
                        logger.error("Unexpected error when deserializing [{}]", rcfModelId);
                        listener.onResponse(Optional.empty());
                        return;
                    }
                    String thresholdingModelId = SingleStreamModelIdMapper.getThresholdModelIdFromRCFModelId(rcfModelId);
                    // query for threshold model and combinne rcf and threshold model into a ThresholdedRandomCutForest
                    getThresholdModel(thresholdingModelId, ActionListener.wrap(thresholdingModel -> {
                        listener.onResponse(checkpointCodec.convertToTRCF(forest.get(), thresholdingModel));
                    }, listener::onFailure));
                }
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Unexpected error when deserializing [{}]", rcfModelId), e);
                listener.onResponse(Optional.empty());
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    /**
     * Returns to listener the checkpoint for the rcf model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    @Override
    public void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, modelId),
                client::get,
                ActionListener.wrap(response -> deserializeTRCFModel(response, modelId, listener), exception -> {
                    // expected exception, don't print stack trace
                    if (exception instanceof IndexNotFoundException) {
                        listener.onResponse(Optional.empty());
                    } else {
                        listener.onFailure(exception);
                    }
                })
            );
    }

    /**
     * Returns to listener the checkpoint for the threshold model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    @Override
    public void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener) {
        clientUtil.<GetRequest, GetResponse>asyncRequest(new GetRequest(indexName, modelId), client::get, ActionListener.wrap(response -> {
            Optional<Object> thresholdCheckpoint = processThresholdModelCheckpoint(response);
            if (!thresholdCheckpoint.isPresent()) {
                listener.onFailure(new ResourceNotFoundException("", "Fail to find model " + modelId));
                return;
            }
            Optional<ThresholdingModel> model = thresholdCheckpoint
                .map(
                    checkpoint -> AccessController
                        .doPrivileged(
                            (PrivilegedAction<ThresholdingModel>) () -> gson.fromJson((String) checkpoint, thresholdingModelClass)
                        )
                );
            listener.onResponse(model);
        }, exception -> {
            // expected exception, don't print stack trace
            if (exception instanceof IndexNotFoundException) {
                listener.onResponse(Optional.empty());
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private Optional<Object> processThresholdModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> source.get(CommonName.FIELD_MODEL));
    }

    @Override
    protected DeleteByQueryRequest createDeleteCheckpointRequest(String detectorId) {
        return new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(ADCommonName.DETECTOR_ID, detectorId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
    }
}
