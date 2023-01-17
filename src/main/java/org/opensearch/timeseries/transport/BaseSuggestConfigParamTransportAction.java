/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.ParseUtils.checkFilterByBackendRoles;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.rest.handler.IntervalCalculation;
import org.opensearch.timeseries.rest.handler.LatestTimeRetriever;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public abstract class BaseSuggestConfigParamTransportAction extends
    HandledTransportAction<SuggestConfigParamRequest, SuggestConfigParamResponse> {
    public static final Logger logger = LogManager.getLogger(BaseSuggestConfigParamTransportAction.class);

    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    protected final SearchFeatureDao searchFeatureDao;
    protected volatile Boolean filterByEnabled;
    protected Clock clock;
    protected AnalysisType context;

    public BaseSuggestConfigParamTransportAction(
        String actionName,
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        Settings settings,
        ActionFilters actionFilters,
        TransportService transportService,
        Setting<Boolean> filterByBackendRoleSetting,
        AnalysisType context,
        SearchFeatureDao searchFeatureDao
    ) {
        super(actionName, transportService, actionFilters, SuggestConfigParamRequest::new);
        this.client = client;
        this.clientUtil = clientUtil;
        this.filterByEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByEnabled = it);
        this.clock = Clock.systemUTC();
        this.context = context;
        this.searchFeatureDao = searchFeatureDao;
    }

    @Override
    protected void doExecute(Task task, SuggestConfigParamRequest request, ActionListener<SuggestConfigParamResponse> listener) {
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(user, listener, () -> suggestExecute(request, user, context, listener));
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    public void resolveUserAndExecute(User requestedUser, ActionListener<SuggestConfigParamResponse> listener, ExecutorFunction function) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users validating detectors who do not have
            // backend roles.
            if (filterByEnabled) {
                String error = checkFilterByBackendRoles(requestedUser);
                if (error != null) {
                    listener.onFailure(new TimeSeriesException(error));
                    return;
                }
            }
            // Validate analysis
            function.execute();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void suggestExecute(
        SuggestConfigParamRequest request,
        User user,
        ThreadContext.StoredContext storedContext,
        ActionListener<SuggestConfigParamResponse> listener
    ) {
        storedContext.restore();
        Config config = request.getConfig();
        if (request.getParam().equals(Forecaster.FORECAST_INTERVAL_FIELD)) {
            IntervalCalculation intervalCalculation = new IntervalCalculation(
                config,
                request.getRequestTimeout(),
                client,
                clientUtil,
                user,
                context,
                clock
            );
            LatestTimeRetriever latestTimeRetriever = new LatestTimeRetriever(
                config,
                request.getRequestTimeout(),
                clientUtil,
                client,
                user,
                context,
                searchFeatureDao
            );

            ActionListener<IntervalTimeConfiguration> intervalSuggestionListener = ActionListener
                .wrap(interval -> listener.onResponse(new SuggestConfigParamResponse(interval)), listener::onFailure);
            ActionListener<Pair<Optional<Long>, Map<String, Object>>> latestTimeListener = ActionListener.wrap(latestEntityAttributes -> {
                Optional<Long> latestTime = latestEntityAttributes.getLeft();
                if (latestTime.isPresent()) {
                    intervalCalculation.findInterval(latestTime.get(), latestEntityAttributes.getRight(), intervalSuggestionListener);
                } else {
                    listener.onFailure(new TimeSeriesException("Empty data. Cannot find a good interval."));
                }

            }, exception -> {
                listener.onFailure(exception);
                logger.error("Failed to create search request for last data point", exception);
            });

            latestTimeRetriever.checkIfHC(latestTimeListener);
        }
    }
}
