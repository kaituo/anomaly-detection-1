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

package org.opensearch.ad.mock.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;

import java.time.Clock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.SdkRunContext;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.transport.JobRequest;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class MockAnomalyDetectorJobTransportActionWithUser extends HandledTransportAction<JobRequest, JobResponse> {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorJobTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private final ADDelegatingDataManagement anomalyDetectionIndices;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private RunContext.RestorableContext context;
    private final ADTaskManager adTaskManager;
    private final TransportService transportService;
    private final ExecuteADResultResponseRecorder recorder;
    private final NodeStateManager nodeStateManager;
    private Clock clock;
    private final RunContext runContext;

    @Inject
    public MockAnomalyDetectorJobTransportActionWithUser(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        ADDelegatingDataManagement anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        ExecuteADResultResponseRecorder recorder,
        NodeStateManager nodeStateManager
    ) {
        super(MockAnomalyDetectorJobAction.NAME, transportService, actionFilters, JobRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        filterByEnabled = AD_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);

        context = () -> {};
        this.runContext = new SdkRunContext();
        this.recorder = recorder;
        this.nodeStateManager = nodeStateManager;
        this.clock = Clock.systemDefaultZone();
    }

    @Override
    protected void doExecute(Task task, JobRequest request, ActionListener<JobResponse> listener) {
        String detectorId = request.getConfigID();
        DateRange detectionDateRange = request.getDateRange();
        boolean historical = request.isHistorical();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = AD_REQUEST_TIMEOUT.get(settings);
        String userStr = "user_name|backendrole1,backendrole2|roles1,role2";
        // By the time request reaches here, the user permissions are validated by Security plugin.
        User user = User.parse(userStr);
        runContext.runWithSystemAuth(() -> resolveUserAndExecute(
            user,
            detectorId,
            filterByEnabled,
            listener,
            (anomalyDetector) -> executeDetector(listener, detectorId, rawPath, requestTimeout, user, detectionDateRange, historical),
            xContentRegistry,
            nodeStateManager,
            anomalyDetectionIndices,
            null,
            AnomalyDetector.class
        ), exception -> {
            logger.error(exception);
            listener.onFailure(exception);
        });
    }

    private void executeDetector(
        ActionListener<JobResponse> listener,
        String detectorId,
        String rawPath,
        TimeValue requestTimeout,
        User user,
        DateRange detectionDateRange,
        boolean historical
    ) {
        ADIndexJobActionHandler handler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            recorder,
            nodeStateManager,
            Settings.EMPTY,
            runContext
        );
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            handler.startConfig(detectorId, null, detectionDateRange, user, transportService, context, clock, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            // Stop detector
            handler.stopConfig(detectorId, null, historical, user, transportService, listener);
        }
    }
}
