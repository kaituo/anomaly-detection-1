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

package org.opensearch.ad;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.commons.authuser.User;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

public class ExecuteADResultResponseRecorder extends
    ExecuteResultResponseRecorder<ADIndex, ADDelegatingDataManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult> {

    private static final Logger log = LogManager.getLogger(ExecuteADResultResponseRecorder.class);

    public ExecuteADResultResponseRecorder(
        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADDelegatingDataManagement> resultHandler,
        ADTaskManager taskManager,
        DiscoveryNodeSelector nodeFilter,
        ThreadPool threadPool,
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager nodeStateManager,
        Clock clock,
        int adResultMappingVersion
    ) {
        super(
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            ADCommonName.AD_THREAD_POOL_NAME,
            nodeCommunicator,
            dataAccess,
            nodeStateManager,
            clock,
            ADIndex.RESULT,
            AnalysisType.AD,
            adResultMappingVersion
        );
    }

    @Override
    protected AnomalyResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeStartTime,
        Instant executeEndTime,
        String errorMessage,
        User user,
        String tenantId
    ) {
        return new AnomalyResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeStartTime,
            executeEndTime,
            errorMessage,
            Optional.empty(), // single-stream detectors have no entity
            user,
            resultMappingVersion,
            null, // no model id
            tenantId
        );
    }

    /**
     * Update real time task (one document per detector in state index). If the real-time task has no changes compared with local cache,
     * the task won't update. Task only updates when the state changed, or any error happened, or job stopped. Task is mainly consumed
     * by the front-end to track analysis status. For single-stream analyses, we embed model total updates in ResultResponse and
     * update state accordingly. For HC analysis, we won't wait for model finishing updating before returning a response to the job scheduler
     * since it might be long before all entities finish execution. So we don't embed model total updates in AnomalyResultResponse.
     * Instead, we issue a profile request to poll each model node and get the maximum total updates among all models.
     * @param response response returned from executing AnomalyResultAction
     * @param configId config Id
     * @param clock Clock to get current time
     */
    @Override
    protected void updateRealtimeTask(ResultResponse<AnomalyResult> response, String configId, String tenantId, Clock clock) {
        if (response.isHC() != null && response.isHC()) {
            if (taskManager.skipUpdateRealtimeTask(configId, response.getError())) {
                return;
            }
            delayedUpdate(response, configId, tenantId, clock);
        } else {
            log
                .debug(
                    "Update latest realtime task for single stream detector {}, total updates: {}",
                    configId,
                    response.getRcfTotalUpdates()
                );
            updateLatestRealtimeTask(
                configId,
                tenantId,
                null,
                response.getRcfTotalUpdates(),
                response.getConfigIntervalInMinutes(),
                response.getError(),
                clock
            );
        }
    }
}
