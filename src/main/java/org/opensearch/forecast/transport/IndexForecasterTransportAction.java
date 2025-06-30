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

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_CREATE_FORECASTER;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_UPDATE_FORECASTER;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.timeseries.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.timeseries.util.ParseUtils.getConfig;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.rest.handler.IndexForecasterActionHandler;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public class IndexForecasterTransportAction extends HandledTransportAction<IndexForecasterRequest, IndexForecasterResponse> {
    private static final Logger LOG = LogManager.getLogger(IndexForecasterTransportAction.class);
    private final TransportService transportService;
    private final ForecastDelegatingDataManagement forecastIndices;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final SearchFeatureDao searchFeatureDao;
    private final ForecastTaskManager taskManager;
    private final Settings settings;
    private final StateManager stateManager;
    private final DataAccess dataAccess;
    private final RunContext runContext;

    @Inject
    public IndexForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Settings settings,
        ForecastDelegatingDataManagement forecastIndices,
        NamedXContentRegistry xContentRegistry,
        SearchFeatureDao searchFeatureDao,
        ForecastTaskManager taskManager,
        StateManager stateManager,
        DataAccess dataAccess,
        RunContext runContext
    ) {
        super(IndexForecasterAction.NAME, transportService, actionFilters, IndexForecasterRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.forecastIndices = forecastIndices;
        this.xContentRegistry = xContentRegistry;
        filterByEnabled = ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.searchFeatureDao = searchFeatureDao;
        this.taskManager = taskManager;
        this.settings = settings;
        this.stateManager = stateManager;
        this.dataAccess = dataAccess;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, IndexForecasterRequest request, ActionListener<IndexForecasterResponse> actionListener) {
        User user = runContext.getUser();
        String forecasterId = request.getForecasterID();
        RestRequest.Method method = request.getMethod();
        String errorMessage = method == RestRequest.Method.PUT ? FAIL_TO_UPDATE_FORECASTER : FAIL_TO_CREATE_FORECASTER;
        ActionListener<IndexForecasterResponse> listener = wrapRestActionListener(actionListener, errorMessage);

        try {
            TenantAwareHelper.validateTenantId(request.getTenantId(), settings, ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext
            .runWithSystemAuth(context -> verifyResourceAccessAndProcessRequest(
                ForecastCommonName.FORECAST_RESOURCE_TYPE,
                () -> indexForecaster(
                    user,
                    method,
                    forecasterId,
                    request.getTenantId(),
                    listener,
                    (forecaster) -> forecastExecute(request, user, forecaster, context, listener)
                ),
                () -> resolveUserAndExecute(
                    user,
                    forecasterId,
                    method,
                    request.getTenantId(),
                    listener,
                    (forecaster) -> forecastExecute(request, user, forecaster, context, listener)
                )
            ), exception -> {
                LOG.error(exception);
                listener.onFailure(exception);
            });
    }

    private void indexForecaster(
        User requestedUser,
        RestRequest.Method method,
        String forecasterId,
        String tenantId,
        ActionListener<IndexForecasterResponse> parentListener,
        Consumer<Forecaster> onFound
    ) {
        if (method == RestRequest.Method.PUT) {
            // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
            // check if request user have access to the forecaster or not. But we still need to get current forecaster for
            // this case, so we can keep current forecaster's user data.
            boolean filterByBackendRole = requestedUser == null ? false : filterByEnabled;

            // Update forecaster request, check if user has permissions to update the forecaster
            // Get forecaster and verify backend roles
            getConfig(
                requestedUser,
                forecasterId,
                parentListener,
                onFound,
                stateManager,
                forecastIndices,
                tenantId,
                filterByBackendRole,
                Forecaster.class
            );
        } else {
            // Create Forecaster. No need to get current
            onFound.accept(null);
        }
    }

    private void resolveUserAndExecute(
        User requestedUser,
        String forecasterId,
        RestRequest.Method method,
        String tenantId,
        ActionListener<IndexForecasterResponse> listener,
        Consumer<Forecaster> function
    ) {
        try {
            if (filterByEnabled) {
                // Check if user has backend roles
                // When filter by is enabled, block users creating/updating detectors who do not have backend roles.
                String error = checkFilterByBackendRoles(requestedUser);
                if (error != null) {
                    listener.onFailure(new IllegalArgumentException(error));
                    return;
                }
            }
            indexForecaster(requestedUser, method, forecasterId, tenantId, listener, function);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void forecastExecute(
        IndexForecasterRequest request,
        User user,
        Forecaster currentForecaster,
        RunContext.RestorableContext storedContext,
        ActionListener<IndexForecasterResponse> listener
    ) {
        forecastIndices.update(request.getTenantId());
        String forecasterId = request.getForecasterID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        Forecaster forecaster = request.getForecaster();
        forecaster.setTenantId(request.getTenantId());
        RestRequest.Method method = request.getMethod();
        TimeValue requestTimeout = request.getRequestTimeout();
        Integer maxSingleStreamForecasters = request.getMaxSingleStreamForecasters();
        Integer maxHCForecasters = request.getMaxHCForecasters();
        Integer maxForecastFeatures = request.getMaxForecastFeatures();
        Integer maxCategoricalFields = request.getMaxCategoricalFields();

        storedContext.restore();
        checkIndicesAndExecute(forecaster.getIndices(), request.getTenantId(), () -> {
            // Don't replace forecaster's user when update config
            // Github issue: https://github.com/opensearch-project/anomaly-detection/issues/124
            User forecastUser = currentForecaster == null ? user : currentForecaster.getUser();
            IndexForecasterActionHandler indexForecasterActionHandler = new IndexForecasterActionHandler(
                clusterService,
                dataAccess,
                transportService,
                forecastIndices,
                forecasterId,
                seqNo,
                primaryTerm,
                refreshPolicy,
                forecaster,
                requestTimeout,
                maxSingleStreamForecasters,
                maxHCForecasters,
                maxForecastFeatures,
                maxCategoricalFields,
                method,
                xContentRegistry,
                forecastUser,
                taskManager,
                searchFeatureDao,
                settings,
                runContext
            );
            indexForecasterActionHandler.start(listener);
        }, listener);
    }

    private void checkIndicesAndExecute(
        List<String> indices,
        String tenantId,
        ExecutorFunction function,
        ActionListener<IndexForecasterResponse> listener
    ) {
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        dataAccess.search(searchRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> { function.execute(); }, e -> {
            // Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
            // https://github.com/opendistro-for-elasticsearch/security/issues/718
            LOG.error(e);
            listener.onFailure(e);
        }));
    }
}
