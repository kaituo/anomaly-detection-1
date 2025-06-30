/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_DELETE_CONFIG;
import static org.opensearch.timeseries.util.ParseUtils.getResourceTypeFromClassName;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.model.ADTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public abstract class BaseDeleteConfigTransportAction<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>, ConfigType extends Config>
    extends HandledTransportAction<DeleteConfigRequest, DeleteResponse> {

    private static final Logger LOG = LogManager.getLogger(BaseDeleteConfigTransportAction.class);

    private final DataAccess taskSearcher;
    private final TransportService transportService;
    private NamedXContentRegistry xContentRegistry;
    private final TaskManagerType taskManager;
    private volatile Boolean filterByEnabled;
    private final StateManager nodeStateManager;
    private final AnalysisType analysisType;
    private final String stateIndex;
    private final Class<ConfigType> configTypeClass;
    private final List<TaskTypeEnum> batchTaskTypes;
    protected final String configIndexName;
    private final DataManagementType dataManagement;
    protected final Settings settings;
    private final RunContext runContext;

    public BaseDeleteConfigTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        StateManager nodeStateManager,
        TaskManagerType taskManager,
        String deleteConfigAction,
        Setting<Boolean> filterByBackendRoleSetting,
        AnalysisType analysisType,
        String stateIndex,
        Class<ConfigType> configTypeClass,
        List<TaskTypeEnum> historicalTaskTypes,
        String configIndexName,
        DataAccess taskSearcher,
        DataManagementType dataManagement,
        RunContext runContext
    ) {
        super(deleteConfigAction, transportService, actionFilters, DeleteConfigRequest::new);
        this.transportService = transportService;
        this.taskSearcher = taskSearcher;
        this.xContentRegistry = xContentRegistry;
        this.taskManager = taskManager;
        this.nodeStateManager = nodeStateManager;
        filterByEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByEnabled = it);

        this.analysisType = analysisType;
        this.stateIndex = stateIndex;
        this.configTypeClass = configTypeClass;
        this.batchTaskTypes = historicalTaskTypes;
        this.configIndexName = configIndexName;
        this.dataManagement = dataManagement;
        this.settings = settings;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, DeleteConfigRequest request, ActionListener<DeleteResponse> actionListener) {
        String configId = request.getConfigID();
        LOG.info("Delete job {}", configId);
        User user = runContext.getUser();
        ActionListener<DeleteResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_CONFIG);

        try {
            TenantAwareHelper.validateTenantId(request.getTenantID(), settings, getMultiTenancyEnabledSetting());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext.runWithSystemAuth(() -> {
            String resourceType = getResourceTypeFromClassName(configTypeClass.getSimpleName());
            verifyResourceAccessAndProcessRequest(
                resourceType,
                () -> deleteConfigIfNotRunning(request, listener),
                () -> resolveUserAndExecute(
                    user,
                    configId,
                    filterByEnabled,
                    listener,
                    (input) -> deleteConfigIfNotRunning(request, listener),
                    xContentRegistry,
                    nodeStateManager,
                    dataManagement,
                    request.getTenantID(),
                    configTypeClass
                )
            );
        }, exception -> {
            LOG.error(exception);
            listener.onFailure(exception);
        });
    }

    private void deleteConfigIfNotRunning(DeleteConfigRequest request, ActionListener<DeleteResponse> listener) {
        String tenantId = request.getTenantID();
        String configId = request.getConfigID();
        nodeStateManager.getConfig(configId, tenantId, analysisType, config -> {
            if (config.isEmpty()) {
                LOG.info("Can't find config {}", configId);
                taskManager.deleteTasks(configId, () -> deleteJobDoc(tenantId, configId, listener), tenantId, listener);
                return;
            }

            // Check if there is a realtime job or batch analysis task running
            getJob(configId, tenantId, listener, () -> {
                taskManager.getAndExecuteOnLatestConfigLevelTask(configId, tenantId, batchTaskTypes, configTask -> {
                    if (configTask.isPresent() && !configTask.get().isDone()) {
                        String batchTaskName = configTask.get() instanceof ADTask ? "Historical" : "Run once";
                        listener.onFailure(new OpenSearchStatusException(batchTaskName + " is running", RestStatus.BAD_REQUEST));
                    } else {
                        taskManager.deleteTasks(configId, () -> deleteJobDoc(tenantId, configId, listener), tenantId, listener);
                    }
                }, transportService, false, listener);
            });
        }, listener);
    }

    protected void deleteJobDoc(String tenantId, String configId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete job {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.JOB_INDEX, configId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        taskSearcher.delete(deleteRequest, TenantContext.user(tenantId), ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                deleteStateDoc(configId, tenantId, listener);
            } else {
                String message = "Fail to delete job " + configId;
                LOG.error(message);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            LOG.error("Failed to delete job for " + configId, exception);
            if (exception instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(exception)) {
                deleteStateDoc(configId, tenantId, listener);
            } else {
                LOG.error("Failed to delete job", exception);
                listener.onFailure(exception);
            }
        }));
    }

    protected void deleteStateDoc(String configId, String tenantId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete config state {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(stateIndex, configId);
        taskSearcher.delete(deleteRequest, TenantContext.user(tenantId), ActionListener.wrap(response -> {
            // whether deleted state doc or not, continue as state doc may not exist
            deleteConfigDoc(configId, tenantId, listener);
        }, exception -> {
            if (exception instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(exception)) {
                deleteConfigDoc(configId, tenantId, listener);
            } else {
                LOG.error("Failed to delete state", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteConfigDoc(String configId, String tenantId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete config {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(configIndexName, configId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        taskSearcher.delete(deleteRequest, TenantContext.user(tenantId), new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void getJob(String configId, String tenantId, ActionListener<DeleteResponse> listener, ExecutorFunction function) {
        if (dataManagement.doesJobIndexExist()) {
            nodeStateManager.getJob(configId, tenantId, false, ActionListener.wrap(jobOptional -> {
                if (jobOptional.isPresent() && jobOptional.get().isEnabled()) {
                    listener.onFailure(new OpenSearchStatusException("Job is running: " + configId, RestStatus.BAD_REQUEST));
                } else {
                    function.execute();
                }
            }, exception -> {
                LOG.error("Fail to get job: " + configId, exception);
                listener.onFailure(exception);
            }));
        } else {
            function.execute();
        }
    }

    /**
     * Returns the setting that indicates if multi-tenancy is enabled.
     * Subclasses must implement this to provide the appropriate setting.
     */
    protected abstract Setting<Boolean> getMultiTenancyEnabledSetting();
}
