/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultRequest;

public class ForecastJobProcessor extends
    JobProcessor<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ExecuteForecastResultResponseRecorder> {

    private static ForecastJobProcessor INSTANCE;

    public static ForecastJobProcessor getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (JobProcessor.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ForecastJobProcessor();
            return INSTANCE;
        }
    }

    private ForecastJobProcessor() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        super(AnalysisType.FORECAST, TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME, ForecastResultAction.INSTANCE);
    }

    public void registerSettings(Settings settings) {
        super.registerSettings(settings, ForecastSettings.FORECAST_MAX_RETRY_FOR_END_RUN_EXCEPTION);
    }

    @Override
    protected ResultRequest createResultRequest(String configId, long start, long end) {
        return new ForecastResultRequest(configId, start, end);
    }
}
