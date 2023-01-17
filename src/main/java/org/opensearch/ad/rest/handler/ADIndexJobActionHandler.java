/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;

import java.util.List;

import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.transport.ResultRequest;

public class ADIndexJobActionHandler extends
    IndexJobActionHandler<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder> {

    public ADIndexJobActionHandler(
        Client client,
        ADIndexManagement indexManagement,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        ExecuteADResultResponseRecorder recorder,
        NodeStateManager nodeStateManager,
        Settings settings
    ) {
        super(
            client,
            indexManagement,
            xContentRegistry,
            adTaskManager,
            recorder,
            AnomalyResultAction.INSTANCE,
            AnalysisType.AD,
            DETECTION_STATE_INDEX,
            StopDetectorAction.INSTANCE,
            nodeStateManager,
            settings,
            AD_REQUEST_TIMEOUT
        );
    }

    @Override
    protected ResultRequest createResultRequest(String configID, long start, long end) {
        return new AnomalyResultRequest(configID, start, end);
    }

    @Override
    protected List<ADTaskType> getHistorialConfigTaskTypes() {
        return HISTORICAL_DETECTOR_TASK_TYPES;
    }
}
