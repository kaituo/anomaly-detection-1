/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_GET_FORECASTER;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.RestHandlerUtils.PROFILE;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public abstract class BaseGetConfigTransportAction<ResponseType extends ActionResponse, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ConfigType extends Config>
    extends HandledTransportAction<GetConfigRequest, ResponseType> {

    private static final Logger LOG = LogManager.getLogger(BaseGetConfigTransportAction.class);

    protected final ClusterService clusterService;
    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    // private final Set<String> allProfileTypeStrs;
    // private final Set<DetectorProfileName> allProfileTypes;
    // private final Set<DetectorProfileName> defaultDetectorProfileTypes;
    // private final Set<String> allEntityProfileTypeStrs;
    // private final Set<EntityProfileName> allEntityProfileTypes;
    // private final Set<EntityProfileName> defaultEntityProfileTypes;
    protected final NamedXContentRegistry xContentRegistry;
    protected final DiscoveryNodeFilterer nodeFilter;
    protected final TransportService transportService;
    protected volatile Boolean filterByEnabled;
    protected final TaskManagerType taskManager;
    private final Class<ConfigType> configTypeClass;
    private final String configParseFieldName;
    private final List<TaskTypeEnum> allTaskTypes;
    private final String singleStreamRealTimeTaskName;
    private final String hcRealTImeTaskName;
    private final String singleStreamHistoricalTaskname;
    private final String hcHistoricalTaskName;

    public BaseGetConfigTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        TaskManagerType forecastTaskManager,
        String getConfigAction,
        Class<ConfigType> configTypeClass,
        String configParseFieldName,
        List<TaskTypeEnum> allTaskTypes,
        String hcRealTImeTaskName,
        String singleStreamRealTimeTaskName,
        String hcHistoricalTaskName,
        String singleStreamHistoricalTaskname,
        Setting<Boolean> filterByBackendRoleEnableSetting
    ) {
        super(getConfigAction, transportService, actionFilters, GetConfigRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.clientUtil = clientUtil;
        // List<DetectorProfileName> allProfiles = Arrays.asList(DetectorProfileName.values());
        // this.allProfileTypes = EnumSet.copyOf(allProfiles);
        // this.allProfileTypeStrs = getProfileListStrs(allProfiles);
        // List<DetectorProfileName> defaultProfiles = Arrays.asList(DetectorProfileName.ERROR, DetectorProfileName.STATE);
        // this.defaultDetectorProfileTypes = new HashSet<DetectorProfileName>(defaultProfiles);
        //
        // List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        // this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        // this.allEntityProfileTypeStrs = getProfileListStrs(allEntityProfiles);
        // List<EntityProfileName> defaultEntityProfiles = Arrays.asList(EntityProfileName.STATE);
        // this.defaultEntityProfileTypes = new HashSet<EntityProfileName>(defaultEntityProfiles);

        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        filterByEnabled = filterByBackendRoleEnableSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleEnableSetting, it -> filterByEnabled = it);
        this.transportService = transportService;
        this.taskManager = forecastTaskManager;
        this.configTypeClass = configTypeClass;
        this.configParseFieldName = configParseFieldName;
        this.allTaskTypes = allTaskTypes;
        this.hcRealTImeTaskName = hcRealTImeTaskName;
        this.singleStreamRealTimeTaskName = singleStreamRealTimeTaskName;
        this.hcHistoricalTaskName = hcHistoricalTaskName;
        this.singleStreamHistoricalTaskname = singleStreamHistoricalTaskname;
    }

    @Override
    protected void doExecute(Task task, GetConfigRequest request, ActionListener<ResponseType> actionListener) {
        String configID = request.getConfigID();
        User user = ParseUtils.getUserContext(client);
        ActionListener<ResponseType> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_FORECASTER);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                configID,
                filterByEnabled,
                listener,
                (config) -> getExecute(request, listener),
                client,
                clusterService,
                xContentRegistry,
                configTypeClass
            );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    protected void getConfigAndJob(
        String configID,
        boolean returnJob,
        boolean returnTask,
        Optional<TaskClass> realtimeConfigTask,
        Optional<TaskClass> historicalConfigTask,
        ActionListener<ResponseType> listener
    ) {
        MultiGetRequest.Item adItem = new MultiGetRequest.Item(CommonName.CONFIG_INDEX, configID);
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(adItem);
        if (returnJob) {
            MultiGetRequest.Item adJobItem = new MultiGetRequest.Item(CommonName.JOB_INDEX, configID);
            multiGetRequest.add(adJobItem);
        }
        client
            .multiGet(
                multiGetRequest,
                onMultiGetResponse(listener, returnJob, returnTask, realtimeConfigTask, historicalConfigTask, configID)
            );
    }

    protected void getExecute(GetConfigRequest request, ActionListener<ResponseType> listener) {
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
                    taskManager.getAndExecuteOnLatestTasks(configID, null, null, allTaskTypes, (taskList) -> {
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
                                taskManager.resetLatestFlagAsFalse(duplicateTasks);
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
                                fillInHistoricalTaskforBwc(tasks, historicalTask);
                            }
                        }
                        getConfigAndJob(configID, returnJob, returnTask, realtimeTask, historicalTask, listener);
                    }, transportService, true, 2, listener);
                } else {
                    getConfigAndJob(configID, returnJob, returnTask, Optional.empty(), Optional.empty(), listener);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(
        ActionListener<ResponseType> listener,
        boolean returnJob,
        boolean returnTask,
        Optional<TaskClass> realtimeTask,
        Optional<TaskClass> historicalTask,
        String configId
    ) {
        return new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetResponse) {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                ConfigType config = null;
                Job job = null;
                String id = null;
                long version = 0;
                long seqNo = 0;
                long primaryTerm = 0;

                for (MultiGetItemResponse response : responses) {
                    if (CommonName.CONFIG_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            listener
                                .onFailure(
                                    new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND)
                                );
                            return;
                        }
                        id = response.getId();
                        version = response.getResponse().getVersion();
                        primaryTerm = response.getResponse().getPrimaryTerm();
                        seqNo = response.getResponse().getSeqNo();
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                config = parser.namedObject(configTypeClass, configParseFieldName, null);
                            } catch (Exception e) {
                                String message = "Failed to parse config " + configId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    } else if (CommonName.JOB_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() != null
                            && response.getResponse().isExists()
                            && !response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                job = Job.parse(parser);
                            } catch (Exception e) {
                                String message = "Failed to parse job " + configId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    }
                }
                listener
                    .onResponse(
                        createResponse(
                            version,
                            id,
                            primaryTerm,
                            seqNo,
                            config,
                            job,
                            returnJob,
                            realtimeTask,
                            historicalTask,
                            returnTask,
                            RestStatus.OK
                        )
                    );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
    }

    protected void fillInHistoricalTaskforBwc(Map<String, TaskClass> tasks, Optional<TaskClass> historicalAdTask) {}

    protected abstract void getExecuteProfile(
        GetConfigRequest request,
        Entity entity,
        String typesStr,
        boolean all,
        String configId,
        ActionListener<ResponseType> listener
    );

    protected abstract ResponseType createResponse(
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
        RestStatus restStatus
    );

    protected OpenSearchStatusException buildInternalServerErrorResponse(Exception e, String errorMsg) {
        LOG.error(errorMsg, e);
        return new OpenSearchStatusException(errorMsg, RestStatus.INTERNAL_SERVER_ERROR);
    }
}
