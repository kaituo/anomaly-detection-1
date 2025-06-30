/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.ParseUtils.getResourceTypeFromClassName;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.time.Clock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public abstract class BaseJobTransportAction<IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>, IndexableResultType extends IndexableResult, ExecuteResultResponseRecorderType extends ExecuteResultResponseRecorder<IndexType, DataManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType>, IndexJobActionHandlerType extends IndexJobActionHandler<IndexType, DataManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ExecuteResultResponseRecorderType>>
    extends HandledTransportAction<JobRequest, JobResponse> {
    private final Logger logger = LogManager.getLogger(BaseJobTransportAction.class);

    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final TransportService transportService;
    private final Setting<TimeValue> requestTimeOutSetting;
    private final String failtoStartMsg;
    private final String failtoStopMsg;
    private final Class<? extends Config> configClass;
    private final IndexJobActionHandlerType indexJobActionHandlerType;
    private final Clock clock;
    private final StateManager stateManager;
    private final DataManagementType dataManagement;
    private final RunContext runContext;

    public BaseJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        Setting<Boolean> filterByBackendRoleSettng,
        String jobActionName,
        Setting<TimeValue> requestTimeOutSetting,
        String failtoStartMsg,
        String failtoStopMsg,
        Class<? extends Config> configClass,
        IndexJobActionHandlerType indexJobActionHandlerType,
        Clock clock,
        StateManager stateManager,
        DataManagementType dataManagement,
        RunContext runContext
    ) {
        super(jobActionName, transportService, actionFilters, JobRequest::new);
        this.transportService = transportService;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        filterByEnabled = filterByBackendRoleSettng.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSettng, it -> filterByEnabled = it);
        this.requestTimeOutSetting = requestTimeOutSetting;
        this.failtoStartMsg = failtoStartMsg;
        this.failtoStopMsg = failtoStopMsg;
        this.configClass = configClass;
        this.indexJobActionHandlerType = indexJobActionHandlerType;
        this.clock = clock;
        this.stateManager = stateManager;
        this.dataManagement = dataManagement;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, JobRequest request, ActionListener<JobResponse> actionListener) {
        String configId = request.getConfigID();
        DateRange dateRange = request.getDateRange();
        boolean historical = request.isHistorical();
        String tenantId = request.getTenantId();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = requestTimeOutSetting.get(settings);
        String errorMessage = rawPath.endsWith(RestHandlerUtils.START_JOB) ? failtoStartMsg : failtoStopMsg;
        ActionListener<JobResponse> listener = wrapRestActionListener(actionListener, errorMessage);

        try {
            TenantAwareHelper.validateTenantId(tenantId, settings, getMultiTenancyEnabledSetting());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        // By the time request reaches here, the user permissions are validated by the Security plugin.
        User user = runContext.getUser();

        runContext.runWithSystemAuth(context -> {
            String resourceType = getResourceTypeFromClassName(configClass.getSimpleName());
            verifyResourceAccessAndProcessRequest(
                resourceType,
                () -> executeConfig(listener, configId, tenantId, dateRange, historical, rawPath, requestTimeout, user, context, clock),
                () -> resolveUserAndExecute(
                    user,
                    configId,
                    filterByEnabled,
                    listener,
                    (config) -> executeConfig(
                        listener,
                        configId,
                        tenantId,
                        dateRange,
                        historical,
                        rawPath,
                        requestTimeout,
                        user,
                        context,
                        clock
                    ),
                    xContentRegistry,
                    stateManager,
                    dataManagement,
                    tenantId,
                    configClass
                )
            );
        }, exception -> {
            logger.error(exception);
            listener.onFailure(exception);
        });
    }

    private void executeConfig(
        ActionListener<JobResponse> listener,
        String configId,
        String tenantId,
        DateRange dateRange,
        boolean historical,
        String rawPath,
        TimeValue requestTimeout,
        User user,
        RunContext.RestorableContext context,
        Clock clock
    ) {
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            indexJobActionHandlerType.startConfig(configId, tenantId, dateRange, user, transportService, context, clock, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            indexJobActionHandlerType.stopConfig(configId, tenantId, historical, user, transportService, listener);
        }
    }

    /**
     * Returns the setting that indicates if multi-tenancy is enabled.
     * Subclasses must implement this to provide the appropriate setting.
     */
    protected abstract Setting<Boolean> getMultiTenancyEnabledSetting();
}
