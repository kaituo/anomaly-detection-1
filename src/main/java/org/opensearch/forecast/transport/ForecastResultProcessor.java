/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_ENTITIES_PER_INTERVAL;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_PAGE_SIZE;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.client.ForecastNodeCommunicator;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportService;

public class ForecastResultProcessor extends
    ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastDelegatingDataManagement, ForecastTaskManager> {

    private static final Logger LOG = LogManager.getLogger(ForecastResultProcessor.class);

    public ForecastResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        StateManager nodeStateManager,
        TransportService transportService,
        ForecastStats timeSeriesStats,
        ForecastTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        DataAccess dataAccess,
        Class<ForecastResultResponse> transportResultResponseClazz,
        FeatureManager featureManager,
        AnalysisType analysisType,
        boolean runOnce,
        DiscoveryNodeSelector discoveryNodeSelector,
        ForecastNodeCommunicator nodeCommunicator
    ) {
        super(
            requestTimeoutSetting,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
            ForecastCommonName.FORECAST_THREAD_POOL_NAME,
            hashRing,
            nodeStateManager,
            transportService,
            timeSeriesStats,
            realTimeTaskManager,
            xContentRegistry,
            dataAccess,
            transportResultResponseClazz,
            featureManager,
            FORECAST_MAX_ENTITIES_PER_INTERVAL,
            FORECAST_PAGE_SIZE,
            analysisType,
            runOnce,
            discoveryNodeSelector,
            nodeCommunicator
        );
    }

    @Override
    protected ForecastResultResponse createResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        String taskId
    ) {
        return new ForecastResultResponse(features, error, rcfTotalUpdates, configInterval, isHC, taskId);
    }

    @Override
    protected void imputeHC(long dataStartTime, long dataEndTime, String configID, String tenantId, String taskId) {
        // no imputation for forecasting as on the fly imputation and error estimation should not mix
    }
}
