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

package org.opensearch.ad.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.transport.BaseDeleteConfigTransportAction;
import org.opensearch.transport.TransportService;

public class DeleteAnomalyDetectorTransportAction extends
    BaseDeleteConfigTransportAction<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADDelegatingDataManagement, ADTaskManager, AnomalyDetector> {

    @Inject
    public DeleteAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        StateManager nodeStateManager,
        ADTaskManager adTaskManager,
        ADDelegatingDataManagement dataManagement,
        RunContext runContext
    ) {
        super(
            transportService,
            actionFilters,
            clusterService,
            settings,
            xContentRegistry,
            nodeStateManager,
            adTaskManager,
            DeleteAnomalyDetectorAction.NAME,
            AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
            AnalysisType.AD,
            ADCommonName.DETECTION_STATE_INDEX,
            AnomalyDetector.class,
            ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES,
            ADIndex.CONFIG.getIndexName(),
            adTaskManager.getDataAccess(),
            dataManagement,
            runContext
        );
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
    }
}
