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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_GET_DETECTOR;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.AnomalyDetectorProfileRunner;
import org.opensearch.ad.EntityProfileRunner;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.BaseGetConfigTransportAction;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

public class GetAnomalyDetectorTransportAction extends
    BaseGetConfigTransportAction<GetAnomalyDetectorResponse, ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskManager, AnomalyDetector> {

    public static final Logger LOG = LogManager.getLogger(GetAnomalyDetectorTransportAction.class);

    private final Set<String> allProfileTypeStrs;
    private final Set<DetectorProfileName> allProfileTypes;
    private final Set<DetectorProfileName> defaultDetectorProfileTypes;
    private final Set<String> allEntityProfileTypeStrs;
    private final Set<EntityProfileName> allEntityProfileTypes;
    private final Set<EntityProfileName> defaultEntityProfileTypes;

    @Inject
    public GetAnomalyDetectorTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
    ) {
        super(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            client,
            clientUtil,
            settings,
            xContentRegistry,
            adTaskManager,
            GetAnomalyDetectorAction.NAME,
            AnomalyDetector.class,
            AnomalyDetector.PARSE_FIELD_NAME,
            ADTaskType.ALL_DETECTOR_TASK_TYPES,
            ADTaskType.AD_REALTIME_HC_DETECTOR.name(),
            ADTaskType.AD_REALTIME_SINGLE_STREAM.name(),
            ADTaskType.AD_HISTORICAL_HC_DETECTOR.name(),
            ADTaskType.AD_HISTORICAL_SINGLE_STREAM.name(),
            AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES
        );

        List<DetectorProfileName> allProfiles = Arrays.asList(DetectorProfileName.values());
        this.allProfileTypes = EnumSet.copyOf(allProfiles);
        this.allProfileTypeStrs = getProfileListStrs(allProfiles);
        List<DetectorProfileName> defaultProfiles = Arrays.asList(DetectorProfileName.ERROR, DetectorProfileName.STATE);
        this.defaultDetectorProfileTypes = new HashSet<DetectorProfileName>(defaultProfiles);

        List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        this.allEntityProfileTypeStrs = getProfileListStrs(allEntityProfiles);
        List<EntityProfileName> defaultEntityProfiles = Arrays.asList(EntityProfileName.STATE);
        this.defaultEntityProfileTypes = new HashSet<EntityProfileName>(defaultEntityProfiles);
    }

    @Override
    protected void doExecute(Task task, GetConfigRequest request, ActionListener<GetAnomalyDetectorResponse> actionListener) {
        String detectorID = request.getConfigID();
        User user = ParseUtils.getUserContext(client);
        ActionListener<GetAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_DETECTOR);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorID,
                filterByEnabled,
                listener,
                (anomalyDetector) -> getExecute(request, listener),
                client,
                clusterService,
                xContentRegistry,
                AnomalyDetector.class
            );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private ActionListener<DetectorProfile> getProfileActionListener(ActionListener<GetAnomalyDetectorResponse> listener) {
        return ActionListener.wrap(new CheckedConsumer<DetectorProfile, Exception>() {
            @Override
            public void accept(DetectorProfile profile) throws Exception {
                listener
                    .onResponse(
                        new GetAnomalyDetectorResponse(0, null, 0, 0, null, null, false, null, null, false, null, profile, null, true)
                    );
            }
        }, exception -> { listener.onFailure(exception); });
    }

    /**
    *
    * @param typesStr a list of input profile types separated by comma
    * @param all whether we should return all profile in the response
    * @return profiles to collect for a detector
    */
    private Set<DetectorProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultDetectorProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return DetectorProfileName.getNames(Sets.intersection(allProfileTypeStrs, typesInRequest));
        }
    }

    /**
     *
     * @param typesStr a list of input profile types separated by comma
     * @param all whether we should return all profile in the response
     * @return profiles to collect for an entity
     */
    private Set<EntityProfileName> getEntityProfilesToCollect(String typesStr, boolean all) {
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

    private Set<String> getProfileListStrs(List<? extends Name> profileList) {
        return profileList.stream().map(profile -> profile.getName()).collect(Collectors.toSet());
    }

    @Override
    protected void fillInHistoricalTaskforBwc(Map<String, ADTask> tasks, Optional<ADTask> historicalAdTask) {
        if (tasks.containsKey(ADTaskType.HISTORICAL.name())) {
            historicalAdTask = Optional.ofNullable(tasks.get(ADTaskType.HISTORICAL.name()));
        }
    }

    @Override
    protected void getExecuteProfile(
        GetConfigRequest request,
        Entity entity,
        String typesStr,
        boolean all,
        String configId,
        ActionListener<GetAnomalyDetectorResponse> listener
    ) {
        if (entity != null) {
            Set<EntityProfileName> entityProfilesToCollect = getEntityProfilesToCollect(typesStr, all);
            EntityProfileRunner profileRunner = new EntityProfileRunner(
                client,
                clientUtil,
                xContentRegistry,
                TimeSeriesSettings.NUM_MIN_SAMPLES
            );
            profileRunner
                .profile(
                    configId,
                    entity,
                    entityProfilesToCollect,
                    ActionListener
                        .wrap(
                            profile -> {
                                listener
                                    .onResponse(
                                        new GetAnomalyDetectorResponse(
                                            0,
                                            null,
                                            0,
                                            0,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null,
                                            profile,
                                            true
                                        )
                                    );
                            },
                            e -> listener.onFailure(e)
                        )
                );
        } else {
            Set<DetectorProfileName> profilesToCollect = getProfilesToCollect(typesStr, all);
            AnomalyDetectorProfileRunner profileRunner = new AnomalyDetectorProfileRunner(
                client,
                clientUtil,
                xContentRegistry,
                nodeFilter,
                TimeSeriesSettings.NUM_MIN_SAMPLES,
                transportService,
                taskManager
            );
            profileRunner.profile(configId, getProfileActionListener(listener), profilesToCollect);
        }
    }

    @Override
    protected GetAnomalyDetectorResponse createResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        AnomalyDetector config,
        Job job,
        boolean returnJob,
        Optional<ADTask> realtimeTask,
        Optional<ADTask> historicalTask,
        boolean returnTask,
        RestStatus restStatus
    ) {
        return new GetAnomalyDetectorResponse(
            version,
            id,
            primaryTerm,
            seqNo,
            config,
            job,
            returnJob,
            realtimeTask.orElse(null),
            historicalTask.orElse(null),
            returnTask,
            RestStatus.OK,
            null,
            null,
            false
        );
    }
}
