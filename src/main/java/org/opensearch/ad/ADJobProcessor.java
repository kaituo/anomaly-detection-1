/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.ResultRequest;

public class ADJobProcessor extends
    JobProcessor<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ExecuteADResultResponseRecorder> {

    private static ADJobProcessor INSTANCE;

    public static ADJobProcessor getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (JobProcessor.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ADJobProcessor();
            return INSTANCE;
        }
    }

    private ADJobProcessor() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        super(AnalysisType.AD, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME, AnomalyResultAction.INSTANCE);
    }

    public void registerSettings(Settings settings) {
        super.registerSettings(settings, AnomalyDetectorSettings.AD_MAX_RETRY_FOR_END_RUN_EXCEPTION);
    }

    @Override
    protected ResultRequest createResultRequest(String configId, long start, long end) {
        return new AnomalyResultRequest(configId, start, end);
    }
}
