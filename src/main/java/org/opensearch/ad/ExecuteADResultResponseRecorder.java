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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.commons.authuser.User;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class ExecuteADResultResponseRecorder extends
    ExecuteResultResponseRecorder<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult> {

    public ExecuteADResultResponseRecorder(
        ADIndexManagement indexManagement,
        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> resultHandler,
        ADTaskManager taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client,
        NodeStateManager nodeStateManager,
        TaskCacheManager taskCacheManager,
        int rcfMinSamples
    ) {
        super(
            indexManagement,
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            client,
            nodeStateManager,
            taskCacheManager,
            rcfMinSamples,
            ADIndex.RESULT,
            AnalysisType.AD
        );
    }

    @Override
    protected AnomalyResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    ) {
        return new AnomalyResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeEndTime,
            Instant.now(),
            errorMessage,
            Optional.empty(), // single-stream detectors have no entity
            user,
            indexManagement.getSchemaVersion(resultIndex),
            null // no model id
        );
    }
}
