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

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.delete.DeleteResponse;
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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.BaseDeleteConfigTransportAction;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.transport.TransportService;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.DeleteScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;

/**
 * Coordinator-node transport that removes external job triggers (e.g., EventBridge Scheduler)
 * before delegating to the common config deletion flow.
 */
public class DeleteAnomalyDetectorMutliTenantTransportAction extends
    BaseDeleteConfigTransportAction<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADDelegatingDataManagement, ADTaskManager, AnomalyDetector> {

    private static final Logger LOG = LogManager.getLogger(DeleteAnomalyDetectorMutliTenantTransportAction.class);

    private final SchedulerClient schedulerClient;
    private final String scheduleGroup;

    @Inject
    public DeleteAnomalyDetectorMutliTenantTransportAction(
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
        this.schedulerClient = initSchedulerClient(settings);
        this.scheduleGroup = EventBridgeHandler
            .resolveConfigScheduleGroup(AnomalyDetectorSettings.AD_SCHEDULER_GROUP.get(settings), AnalysisType.AD);
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    private SchedulerClient initSchedulerClient(Settings settings) {
        String region = TimeSeriesSettings.REGION.get(settings);
        if (region == null || region.isBlank()) {
            LOG.warn("plugins.timeseries.region is not set; falling back to index-based job deletion.");
            return null;
        }

        try {
            String normalizedRegion = region.trim();
            return AccessController
                .doPrivileged(
                    (PrivilegedAction<SchedulerClient>) () -> SchedulerClient
                        .builder()
                        .region(Region.of(normalizedRegion))
                        .credentialsProvider(SecurityUtil.createCredentialsProvider())
                        .build()
                );
        } catch (Exception e) {
            LOG.warn("Failed to initialize EventBridge Scheduler client; falling back to index-based job deletion.", e);
            return null;
        }
    }

    @Override
    protected void deleteJobDoc(String tenantId, String configId, ActionListener<DeleteResponse> listener) {
        if (schedulerClient == null) {
            LOG.warn("Scheduler client unavailable; using index-based job deletion for config {}", configId);
            super.deleteJobDoc(tenantId, configId, listener);
            return;
        }

        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, configId);
        try {
            schedulerClient.deleteSchedule(DeleteScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build());
            LOG.info("Removed EventBridge Scheduler trigger {} for config {}", scheduleName, configId);
            deleteStateDoc(configId, tenantId, listener);
        } catch (ResourceNotFoundException notFoundException) {
            LOG.info("EventBridge Scheduler trigger {} not found for config {}", scheduleName, configId);
            deleteStateDoc(configId, tenantId, listener);
        } catch (Exception e) {
            String message = "Failed to delete EventBridge Scheduler trigger " + scheduleName;
            LOG.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            return;
        }
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
    }
}
