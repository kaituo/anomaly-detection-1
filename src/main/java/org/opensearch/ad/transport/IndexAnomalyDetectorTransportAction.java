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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_CREATE_DETECTOR;
import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_UPDATE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.timeseries.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.timeseries.util.ParseUtils.getConfig;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public class IndexAnomalyDetectorTransportAction extends HandledTransportAction<IndexAnomalyDetectorRequest, IndexAnomalyDetectorResponse> {
    private static final Logger LOG = LogManager.getLogger(IndexAnomalyDetectorTransportAction.class);
    private final TransportService transportService;
    private final ADDelegatingDataManagement anomalyDetectionIndices;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final ADTaskManager adTaskManager;
    private volatile Boolean filterByEnabled;
    private final SearchFeatureDao searchFeatureDao;
    private final Settings settings;
    private final DataAccess dataAccess;
    private final StateManager stateManager;
    private final RunContext runContext;

    @Inject
    public IndexAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Settings settings,
        ADDelegatingDataManagement anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        DataAccess dataAccess,
        StateManager stateManager,
        RunContext runContext
    ) {
        super(IndexAnomalyDetectorAction.NAME, transportService, actionFilters, IndexAnomalyDetectorRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        this.searchFeatureDao = searchFeatureDao;
        filterByEnabled = AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.settings = settings;
        this.dataAccess = dataAccess;
        this.stateManager = stateManager;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, IndexAnomalyDetectorRequest request, ActionListener<IndexAnomalyDetectorResponse> actionListener) {
        User user = runContext.getUser();
        String detectorId = request.getDetectorID();
        RestRequest.Method method = request.getMethod();
        String errorMessage = method == RestRequest.Method.PUT ? FAIL_TO_UPDATE_DETECTOR : FAIL_TO_CREATE_DETECTOR;
        ActionListener<IndexAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, errorMessage);

        try {
            TenantAwareHelper.validateTenantId(request.getTenantId(), settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext
            .runWithSystemAuth(
                context -> verifyResourceAccessAndProcessRequest(
                    ADCommonName.AD_RESOURCE_TYPE,
                    () -> indexDetector(
                        user,
                        detectorId,
                        method,
                        request.getTenantId(),
                        listener,
                        detector -> adExecute(request, user, detector, context, request.getTenantId(), listener)
                    ),
                    () -> resolveUserAndExecute(
                        user,
                        detectorId,
                        method,
                        request.getTenantId(),
                        listener,
                        (detector) -> adExecute(request, user, detector, context, request.getTenantId(), listener)
                    )
                ),
                exception -> {
                    LOG.error(exception);
                    listener.onFailure(exception);
                }
            );
    }

    private void resolveUserAndExecute(
        User requestedUser,
        String detectorId,
        RestRequest.Method method,
        String tenantId,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        Consumer<AnomalyDetector> function
    ) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users creating/updating detectors who do not have backend roles.
            if (filterByEnabled) {
                String error = checkFilterByBackendRoles(requestedUser);
                if (error != null) {
                    listener.onFailure(new TimeSeriesException(error));
                    return;
                }
            }

            indexDetector(requestedUser, detectorId, method, tenantId, listener, function);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void indexDetector(
        User requestedUser,
        String detectorId,
        RestRequest.Method method,
        String tenantId,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        Consumer<AnomalyDetector> function
    ) {
        if (method == RestRequest.Method.PUT) {
            // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
            // check if request user have access to the detector or not. But we still need to get current detector for
            // this case, so we can keep current detector's user data.
            boolean filterByBackendRole = requestedUser == null ? false : filterByEnabled;
            // Update detector request, check if user has permissions to update the detector
            // Get detector and verify backend roles
            getConfig(
                requestedUser,
                detectorId,
                listener,
                function,
                stateManager,
                anomalyDetectionIndices,
                tenantId,
                filterByBackendRole,
                AnomalyDetector.class
            );
        } else {
            // Create Detector. No need to get current detector.
            function.accept(null);
        }
    }

    protected void adExecute(
        IndexAnomalyDetectorRequest request,
        User user,
        AnomalyDetector currentDetector,
        RunContext.RestorableContext storedContext,
        String tenantId,
        ActionListener<IndexAnomalyDetectorResponse> listener
    ) {
        anomalyDetectionIndices.update(tenantId);
        String detectorId = request.getDetectorID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        AnomalyDetector detector = request.getDetector();
        // tenant id is not part of the detector object and is part of http headers, so we need to set it separately
        detector.setTenantId(tenantId);
        RestRequest.Method method = request.getMethod();
        TimeValue requestTimeout = request.getRequestTimeout();
        Integer maxSingleEntityAnomalyDetectors = request.getMaxSingleEntityAnomalyDetectors();
        Integer maxMultiEntityAnomalyDetectors = request.getMaxMultiEntityAnomalyDetectors();
        Integer maxAnomalyFeatures = request.getMaxAnomalyFeatures();
        Integer maxCategoricalFields = request.getMaxCategoricalFields();

        try {
            applyEventBridgeCellId(method, detector, currentDetector);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        storedContext.restore();
        checkIndicesAndExecute(detector.getIndices(), () -> {
            // Don't replace detector's user when update detector
            // Github issue: https://github.com/opensearch-project/anomaly-detection/issues/124
            // TODO this and similar code should be updated to remove reference to a user

            User detectorUser = currentDetector == null ? user : currentDetector.getUser();
            IndexAnomalyDetectorActionHandler indexAnomalyDetectorActionHandler = new IndexAnomalyDetectorActionHandler(
                clusterService,
                dataAccess,
                transportService,
                anomalyDetectionIndices,
                detectorId,
                seqNo,
                primaryTerm,
                refreshPolicy,
                detector,
                requestTimeout,
                maxSingleEntityAnomalyDetectors,
                maxMultiEntityAnomalyDetectors,
                maxAnomalyFeatures,
                maxCategoricalFields,
                method,
                xContentRegistry,
                detectorUser,
                adTaskManager,
                searchFeatureDao,
                settings,
                runContext
            );
            indexAnomalyDetectorActionHandler.start(listener);
        }, tenantId, listener);
    }

    void applyEventBridgeCellId(RestRequest.Method method, AnomalyDetector detector, AnomalyDetector currentDetector) {
        if (!AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)) {
            return;
        }

        if (method == RestRequest.Method.PUT) {
            detector.setEventBridgeCellId(currentDetector == null ? null : currentDetector.getEventBridgeCellId());
            return;
        }

        String transientKey = AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_HEADER_NAME.get(settings);
        if (transientKey == null || transientKey.trim().isEmpty()) {
            throw new OpenSearchStatusException(
                AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_HEADER_NAME.getKey() + " must be configured in AD multi-tenancy mode",
                RestStatus.BAD_REQUEST
            );
        }

        Object threadContextValue = transportService.getThreadPool().getThreadContext().getTransient(transientKey.trim());
        String eventBridgeCellId = threadContextValue == null ? null : threadContextValue.toString().trim();
        if (eventBridgeCellId == null || eventBridgeCellId.isEmpty()) {
            throw new OpenSearchStatusException(
                "Missing thread context transient for eventBridgeCellId using key [" + transientKey + "]",
                RestStatus.BAD_REQUEST
            );
        }

        detector.setEventBridgeCellId(eventBridgeCellId);
    }

    private void checkIndicesAndExecute(
        List<String> indices,
        ExecutorFunction function,
        String tenantId,
        ActionListener<IndexAnomalyDetectorResponse> listener
    ) {
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        dataAccess.search(searchRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            LOG.info("Source index validation succeeded for indices {} tenant {}", indices, tenantId);
            function.execute();
        }, e -> {
            // Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
            // https://github.com/opendistro-for-elasticsearch/security/issues/718
            String message = "Source index validation failed for indices " + indices + " tenant " + tenantId + ": " + e.getMessage();
            LOG.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
        }));
    }
}
