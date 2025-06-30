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

package org.opensearch.forecast.ml;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.client.Client;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.state.RCFCasterMapper;
import com.amazon.randomcutforest.parkservices.state.RCFCasterState;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;

/**
 * The ForecastCheckpointDao class implements all the functionality required for fetching, updating and
 * removing forecast checkpoints.
 *
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ForecastCheckpointDao extends
    CheckpointDao<RCFCaster, ForecastIndex, ForecastDelegatingDataManagement, ForecastCheckpointCodec> {
    public static final Logger logger = LogManager.getLogger(ForecastCheckpointDao.class);

    static final String NOT_ABLE_TO_DELETE_CHECKPOINT_MSG = "Cannot delete all checkpoints of forecaster";

    RCFCasterMapper mapper;
    private Clock clock;

    public ForecastCheckpointDao(
        Client client,
        ClientUtil clientUtil,
        Gson gson,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        ForecastDelegatingDataManagement dataManagement,
        RCFCasterMapper mapper,
        Schema<RCFCasterState> rcfCasterSchema,
        Clock clock
    ) {
        super(
            client,
            clientUtil,
            ForecastIndex.CHECKPOINT.getIndexName(),
            dataManagement,
            new ForecastCheckpointCodec(
                clock,
                maxCheckpointBytes,
                mapper,
                rcfCasterSchema,
                serializeRCFBufferPool,
                serializeRCFBufferSize,
                dataManagement
            )
        );
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * Puts a RCFCaster model checkpoint in the storage. Used in single-stream forecasting.
     *
     * @param modelId id of the model
     * @param caster the RCFCaster model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putCasterCheckpoint(String modelId, RCFCaster caster, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        Optional<String> modelCheckpoint = checkpointCodec.toCheckpoint(Optional.of(caster));
        if (modelCheckpoint.isPresent()) {
            source.put(CommonName.FIELD_MODEL, modelCheckpoint.get());
            source.put(ForecastCommonName.FORECASTER_ID_KEY, SingleStreamModelIdMapper.getConfigIdForModelId(modelId));
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
            source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ForecastIndex.CHECKPOINT));
            putModelCheckpoint(modelId, source, listener);
        } else {
            listener.onFailure(new RuntimeException("Fail to create checkpoint to save"));
        }
    }

    /**
     * Delete checkpoints associated with a forecaster.  Used in HC forecaster.
     * @param forecasterId Forecaster Id
     */
    public void deleteModelCheckpointByForecasterId(String forecasterId) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(ForecastCommonName.FORECASTER_ID_KEY, forecasterId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
        logger.info("Delete checkpoints of forecaster {}", forecasterId);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, forecasterId);
            }
            // can return 0 docs get deleted because:
            // 1) we cannot find matching docs
            // 2) bad stats from OpenSearch. In this case, docs are deleted, but
            // OpenSearch says deleted is 0.
            logger.info("{} " + CheckpointDao.DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(CheckpointDao.INDEX_DELETED_LOG_MSG + " {}", forecasterId);
            } else {
                // Gonna eventually delete in daily cron.
                logger.error(NOT_ABLE_TO_DELETE_CHECKPOINT_MSG, exception);
            }
        }));
    }

    @Override
    protected DeleteByQueryRequest createDeleteCheckpointRequest(String configId) {
        return new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(ForecastCommonName.FORECASTER_ID_KEY, configId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
    }
}
