/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_GET_CONFIG_MSG;
import static org.opensearch.timeseries.util.ParseUtils.getResourceTypeFromClassName;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.PROFILE;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TaskProfileRunner;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

public abstract class BaseGetConfigTransportAction<GetConfigResponseType extends ActionResponse, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>, ConfigType extends Config, EntityProfileRunnerType extends EntityProfileRunner, TaskProfileType extends TaskProfile<TaskClass>, ConfigProfileType extends ConfigProfile<TaskClass, TaskProfileType>, ProfileActionType extends ActionType<ProfileResponse>, TaskProfileRunnerType extends TaskProfileRunner<TaskClass, TaskProfileType>, ProfileRunnerType extends ProfileRunner<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, DataManagementType, TaskProfileType, TaskManagerType, ConfigProfileType, ProfileActionType, TaskProfileRunnerType>>
    extends HandledTransportAction<ActionRequest, GetConfigResponseType> {

    private static final Logger LOG = LogManager.getLogger(BaseGetConfigTransportAction.class);

    protected final ClusterService clusterService;
    protected final DataAccess dataAccess;
    protected final StateManager stateManager;
    protected final NodeCommunicator nodeCommunicator;
    protected final Set<String> allProfileTypeStrs;
    protected final Set<ProfileName> allProfileTypes;
    protected final Set<ProfileName> defaultDetectorProfileTypes;
    protected final Set<String> allEntityProfileTypeStrs;
    protected final Set<EntityProfileName> allEntityProfileTypes;
    protected final Set<EntityProfileName> defaultEntityProfileTypes;
    protected final NamedXContentRegistry xContentRegistry;
    protected final DiscoveryNodeSelector nodeFilter;
    protected final TransportService transportService;
    protected volatile Boolean filterByEnabled;
    protected final TaskManagerType taskManager;
    private final Class<ConfigType> configTypeClass;
    private final List<TaskTypeEnum> allTaskTypes;
    private final String singleStreamRealTimeTaskName;
    private final String hcRealTImeTaskName;
    private final String singleStreamHistoricalTaskname;
    private final String hcHistoricalTaskName;
    private final TaskProfileRunnerType taskProfileRunner;
    protected final String configIndexName;
    protected final DataManagementType dataManagement;
    protected final Settings settings;
    protected final RunContext runContext;

    public BaseGetConfigTransportAction(
        TransportService transportService,
        DiscoveryNodeSelector nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        DataAccess dataAccess,
        StateManager stateManager,
        NodeCommunicator nodeCommunicator,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        TaskManagerType forecastTaskManager,
        String getConfigAction,
        Class<ConfigType> configTypeClass,
        List<TaskTypeEnum> allTaskTypes,
        String hcRealTImeTaskName,
        String singleStreamRealTimeTaskName,
        String hcHistoricalTaskName,
        String singleStreamHistoricalTaskname,
        Setting<Boolean> filterByBackendRoleEnableSetting,
        TaskProfileRunnerType taskProfileRunner,
        String configIndexName,
        DataManagementType dataManagement,
        RunContext runContext
    ) {
        super(getConfigAction, transportService, actionFilters, GetConfigRequest::new);
        this.clusterService = clusterService;
        this.dataAccess = dataAccess;
        this.stateManager = stateManager;
        this.nodeCommunicator = nodeCommunicator;

        List<ProfileName> allProfiles = Arrays.asList(ProfileName.values());
        this.allProfileTypes = EnumSet.copyOf(allProfiles);
        this.allProfileTypeStrs = Name.getListStrs(allProfiles);
        List<ProfileName> defaultProfiles = Arrays.asList(ProfileName.ERROR, ProfileName.STATE);
        this.defaultDetectorProfileTypes = new HashSet<>(defaultProfiles);

        List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        this.allEntityProfileTypeStrs = Name.getListStrs(allEntityProfiles);
        List<EntityProfileName> defaultEntityProfiles = Arrays.asList(EntityProfileName.STATE);
        this.defaultEntityProfileTypes = new HashSet<>(defaultEntityProfiles);

        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        filterByEnabled = filterByBackendRoleEnableSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleEnableSetting, it -> filterByEnabled = it);
        this.transportService = transportService;
        this.taskManager = forecastTaskManager;
        this.configTypeClass = configTypeClass;
        this.allTaskTypes = allTaskTypes;
        this.hcRealTImeTaskName = hcRealTImeTaskName;
        this.singleStreamRealTimeTaskName = singleStreamRealTimeTaskName;
        this.hcHistoricalTaskName = hcHistoricalTaskName;
        this.singleStreamHistoricalTaskname = singleStreamHistoricalTaskname;
        this.taskProfileRunner = taskProfileRunner;
        this.configIndexName = configIndexName;
        this.dataManagement = dataManagement;
        this.settings = settings;
        this.runContext = runContext;
    }

    @Override
    public void doExecute(Task task, ActionRequest request, ActionListener<GetConfigResponseType> actionListener) {
        GetConfigRequest getConfigRequest = GetConfigRequest.fromActionRequest(request);
        String configID = getConfigRequest.getConfigID();
        User user = runContext.getUser();
        ActionListener<GetConfigResponseType> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_CONFIG_MSG);

        try {
            TenantAwareHelper.validateTenantId(getConfigRequest.getTenantId(), settings, getMultiTenancyEnabledSetting());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext.runWithSystemAuth(() -> {
            String resourceType = getResourceTypeFromClassName(configTypeClass.getSimpleName());
            verifyResourceAccessAndProcessRequest(
                resourceType,
                () -> getExecute(getConfigRequest, listener),
                () -> resolveUserAndExecute(
                    user,
                    configID,
                    filterByEnabled,
                    listener,
                    (config) -> getExecute(getConfigRequest, listener),
                    xContentRegistry,
                    stateManager,
                    dataManagement,
                    getConfigRequest.getTenantId(),
                    configTypeClass
                )
            );
        }, exception -> {
            LOG.error(exception);
            listener.onFailure(exception);
        });
    }

    public void getConfigAndJob(
        String configID,
        boolean returnJob,
        boolean returnTask,
        Optional<TaskClass> realtimeConfigTask,
        Optional<TaskClass> historicalConfigTask,
        String tenantId,
        ActionListener<GetConfigResponseType> listener
    ) {
        AnalysisType context = configIndexName.equals(ADCommonName.CONFIG_INDEX) ? AnalysisType.AD : AnalysisType.FORECAST;
        stateManager.getConfig(configID, tenantId, context, false, ActionListener.wrap(configOptional -> {
            if (configOptional.isEmpty()) {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configID, RestStatus.NOT_FOUND));
                return;
            }

            ConfigType config = (ConfigType) configOptional.get();
            ActionListener<Job> jobListener = ActionListener.wrap(job -> {
                adjustState(realtimeConfigTask, job);
                adjustState(historicalConfigTask, job);
                listener
                    .onResponse(
                        createResponse(
                            config.getVersion() == null ? 0 : config.getVersion(),
                            config.getId(),
                            0,
                            0,
                            config,
                            job,
                            returnJob,
                            realtimeConfigTask,
                            historicalConfigTask,
                            returnTask,
                            RestStatus.OK,
                            null,
                            null,
                            false
                        )
                    );
            }, listener::onFailure);

            if (returnJob) {
                stateManager
                    .getJob(
                        configID,
                        tenantId,
                        false,
                        ActionListener.wrap(jobOptional -> jobListener.onResponse(jobOptional.orElse(null)), listener::onFailure)
                    );
            } else {
                jobListener.onResponse(null);
            }
        }, listener::onFailure));
    }

    public void getExecute(GetConfigRequest request, ActionListener<GetConfigResponseType> listener) {
        String configID = request.getConfigID();
        String typesStr = request.getTypeStr();
        String rawPath = request.getRawPath();
        Entity entity = request.getEntity();
        boolean all = request.isAll();
        boolean returnJob = request.isReturnJob();
        boolean returnTask = request.isReturnTask();

        try {
            if (!Strings.isEmpty(typesStr) || rawPath.endsWith(PROFILE) || rawPath.endsWith(PROFILE + "/")) {
                getExecuteProfile(request, entity, typesStr, all, configID, listener);
            } else {
                if (returnTask) {
                    taskManager.getAndExecuteOnLatestTasks(configID, null, null, request.getTenantId(), allTaskTypes, (taskList) -> {
                        Optional<TaskClass> realtimeTask = Optional.empty();
                        Optional<TaskClass> historicalTask = Optional.empty();
                        if (taskList != null && taskList.size() > 0) {
                            Map<String, TaskClass> tasks = new HashMap<>();
                            List<TaskClass> duplicateTasks = new ArrayList<>();
                            for (TaskClass task : taskList) {
                                if (tasks.containsKey(task.getTaskType())) {
                                    LOG
                                        .info(
                                            "Found duplicate latest task of config {}, task id: {}, task type: {}",
                                            configID,
                                            task.getTaskType(),
                                            task.getTaskId()
                                        );
                                    duplicateTasks.add(task);
                                    continue;
                                }
                                tasks.put(task.getTaskType(), task);
                            }
                            if (duplicateTasks.size() > 0) {
                                taskManager.resetLatestFlagAsFalse(duplicateTasks, request.getTenantId());
                            }

                            if (tasks.containsKey(hcRealTImeTaskName)) {
                                realtimeTask = Optional.ofNullable(tasks.get(hcRealTImeTaskName));
                            } else if (tasks.containsKey(singleStreamRealTimeTaskName)) {
                                realtimeTask = Optional.ofNullable(tasks.get(singleStreamRealTimeTaskName));
                            }
                            if (tasks.containsKey(hcHistoricalTaskName)) {
                                historicalTask = Optional.ofNullable(tasks.get(hcHistoricalTaskName));
                            } else if (tasks.containsKey(singleStreamHistoricalTaskname)) {
                                historicalTask = Optional.ofNullable(tasks.get(singleStreamHistoricalTaskname));
                            } else {
                                // AD needs to provides custom behavior for bwc, while forecasting can inherit
                                // the empty implementation
                                historicalTask = fillInHistoricalTaskforBwc(tasks);
                            }
                        }
                        getConfigAndJob(configID, returnJob, returnTask, realtimeTask, historicalTask, request.getTenantId(), listener);
                    }, transportService, false, 2, listener); // false means not reset task state to stopped state
                } else {
                    getConfigAndJob(configID, returnJob, returnTask, Optional.empty(), Optional.empty(), request.getTenantId(), listener);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    protected Optional<TaskClass> fillInHistoricalTaskforBwc(Map<String, TaskClass> tasks) {
        return Optional.empty();
    }

    protected void getExecuteProfile(
        GetConfigRequest request,
        Entity entity,
        String typesStr,
        boolean all,
        String configId,
        ActionListener<GetConfigResponseType> listener
    ) {
        if (entity != null) {
            Set<EntityProfileName> entityProfilesToCollect = getEntityProfilesToCollect(typesStr, all);
            EntityProfileRunnerType profileRunner = createEntityProfileRunner(
                nodeCommunicator,
                dataAccess,
                stateManager,
                xContentRegistry
            );
            profileRunner.profile(configId, request.getTenantId(), entity, entityProfilesToCollect, ActionListener.wrap(profile -> {
                listener
                    .onResponse(
                        createResponse(
                            0,
                            null,
                            0,
                            0,
                            null,
                            null,
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            null,
                            null,
                            profile,
                            true
                        )
                    );
            }, e -> listener.onFailure(e)));
        } else {
            Set<ProfileName> profilesToCollect = getProfilesToCollect(typesStr, all);
            ProfileRunnerType profileRunner = createProfileRunner(
                nodeCommunicator,
                dataAccess,
                xContentRegistry,
                nodeFilter,
                transportService,
                taskManager,
                taskProfileRunner
            );
            profileRunner.profile(configId, request.getTenantId(), getProfileActionListener(listener), profilesToCollect);
        }

    }

    protected abstract GetConfigResponseType createResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        ConfigType config,
        Job job,
        boolean returnJob,
        Optional<TaskClass> realtimeTask,
        Optional<TaskClass> historicalTask,
        boolean returnTask,
        RestStatus restStatus,
        ConfigProfileType detectorProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    );

    protected OpenSearchStatusException buildInternalServerErrorResponse(Exception e, String errorMsg) {
        LOG.error(errorMsg, e);
        return new OpenSearchStatusException(errorMsg, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     *
     * @param typesStr a list of input profile types separated by comma
     * @param all whether we should return all profile in the response
     * @return profiles to collect for an entity
     */
    protected Set<EntityProfileName> getEntityProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allEntityProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultEntityProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return EntityProfileName.getNames(Sets.intersection(allEntityProfileTypeStrs, typesInRequest));
        }
    }

    /**
    *
    * @param typesStr a list of input profile types separated by comma
    * @param all whether we should return all profile in the response
    * @return profiles to collect for a detector
    */
    protected Set<ProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultDetectorProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return ProfileName.getNames(Sets.intersection(allProfileTypeStrs, typesInRequest));
        }
    }

    protected ActionListener<ConfigProfileType> getProfileActionListener(ActionListener<GetConfigResponseType> listener) {
        return ActionListener.wrap(new CheckedConsumer<ConfigProfileType, Exception>() {
            @Override
            public void accept(ConfigProfileType profile) throws Exception {
                listener
                    .onResponse(
                        createResponse(
                            0,
                            null,
                            0,
                            0,
                            null,
                            null,
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            null,
                            profile,
                            null,
                            true
                        )
                    );
            }
        }, exception -> { listener.onFailure(exception); });
    }

    protected abstract void adjustState(Optional<TaskClass> taskOptional, Job job);

    protected abstract EntityProfileRunnerType createEntityProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager stateManager,
        NamedXContentRegistry xContentRegistry
    );

    protected abstract ProfileRunnerType createProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeSelector nodeFilter,
        TransportService transportService,
        TaskManagerType taskManager,
        TaskProfileRunnerType taskProfileRunner
    );

    /**
     * Returns the setting that indicates if multi-tenancy is enabled.
     * Subclasses must implement this to provide the appropriate setting.
     */
    protected abstract Setting<Boolean> getMultiTenancyEnabledSetting();
}
