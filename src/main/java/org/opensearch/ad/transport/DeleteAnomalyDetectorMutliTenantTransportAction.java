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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
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
import org.opensearch.ad.sqs.ADEventBridgeTargetResolver;
import org.opensearch.ad.sqs.ADSqsAccountTarget;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
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

    private final Map<String, SchedulerClient> schedulerClients = new ConcurrentHashMap<>();
    private final StateManager stateManager;
    private final ADEventBridgeTargetResolver targetResolver;
    private final String scheduleGroup;
    private final String region;

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
        this.stateManager = nodeStateManager;
        this.region = TimeSeriesSettings.REGION.get(settings);
        this.targetResolver = initTargetResolver(settings);
        this.scheduleGroup = EventBridgeHandler
            .resolveConfigScheduleGroup(AnomalyDetectorSettings.AD_SCHEDULER_GROUP.get(settings), AnalysisType.AD);
    }

    private ADEventBridgeTargetResolver initTargetResolver(Settings settings) {
        if (region == null || region.isBlank()) {
            LOG.warn("plugins.timeseries.region is not set; falling back to index-based job deletion.");
            return null;
        }

        try {
            JobQueueAccountIdProvider accountProvider = JobQueueAccountIdProvider
                .find(TimeSeriesSettings.SQS_ACCOUNT_PROVIDER_TYPE.get(settings), settings);
            return new ADEventBridgeTargetResolver(settings, accountProvider);
        } catch (Exception e) {
            LOG.warn("Failed to initialize EventBridge target resolver; falling back to index-based job deletion.", e);
            return null;
        }
    }

    @Override
    protected void deleteJobDoc(String tenantId, String configId, ActionListener<DeleteResponse> listener) {
        if (targetResolver == null) {
            LOG.warn("Scheduler client unavailable; using index-based job deletion for config {}", configId);
            super.deleteJobDoc(tenantId, configId, listener);
            return;
        }

        stateManager.getConfig(configId, tenantId, AnalysisType.AD, config -> {
            String eventBridgeAccountId = config.isPresent() ? Strings.trimToNull(config.get().getEventBridgeCellId()) : null;
            if (eventBridgeAccountId == null) {
                LOG.info("Config {} has no EventBridge account recorded; skipping schedule deletion.", configId);
                deleteStateDoc(configId, tenantId, listener);
                return;
            }
            deleteScheduleForAccount(eventBridgeAccountId, tenantId, configId, listener);
        }, listener);
    }

    private void deleteScheduleForAccount(String accountId, String tenantId, String configId, ActionListener<DeleteResponse> listener) {
        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, configId);
        try {
            ADSqsAccountTarget target = targetResolver.resolveAccount(accountId);
            schedulerClientFor(target).deleteSchedule(DeleteScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build());
            LOG.info("Removed EventBridge Scheduler trigger {} for config {} in account {}", scheduleName, configId, accountId);
            deleteStateDoc(configId, tenantId, listener);
        } catch (ResourceNotFoundException notFoundException) {
            LOG.info("EventBridge Scheduler trigger {} not found for config {} in account {}", scheduleName, configId, accountId);
            deleteStateDoc(configId, tenantId, listener);
        } catch (Exception e) {
            String message = "Failed to delete EventBridge Scheduler trigger " + scheduleName + " in account " + accountId;
            LOG.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
        }
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    private SchedulerClient schedulerClientFor(ADSqsAccountTarget target) {
        return schedulerClients
            .computeIfAbsent(
                target.getAccountId(),
                accountId -> AccessController
                    .doPrivileged(
                        (PrivilegedAction<SchedulerClient>) () -> SchedulerClient
                            .builder()
                            .region(Region.of(region.trim()))
                            .credentialsProvider(
                                target.getScheduleManagementRoleArn() == null || target.getScheduleManagementRoleArn().isBlank()
                                    ? SecurityUtil.createCredentialsProvider()
                                    : SecurityUtil
                                        .createAssumeRoleCredentialsProvider(
                                            region.trim(),
                                            target.getScheduleManagementRoleArn(),
                                            "ad-scheduler-" + accountId
                                        )
                            )
                            .build()
                    )
            );
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
    }
}
