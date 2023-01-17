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

package org.opensearch.forecast;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_BACKOFF_MINUTES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_RETRY_FOR_UNRESPONSIVE_NODE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.ExceptionUtil;

public class ForecastNodeStateManager extends TimeSeriesNodeStateManager<ForecastNodeState> {
    private static final Logger LOG = LogManager.getLogger(ForecastNodeStateManager.class);

    /**
     * Constructor
     *
     * @param client Client to make calls to OpenSearch
     * @param xContentRegistry OS named content registry
     * @param settings OS settings
     * @param clientUtil AD Client utility
     * @param clock A UTC clock
     * @param stateTtl Max time to keep state in memory
     * @param clusterService Cluster service accessor
     */
    public ForecastNodeStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl,
        ClusterService clusterService
    ) {
        super(client, xContentRegistry, settings, clientUtil, clock, stateTtl, clusterService, FORECAST_MAX_RETRY_FOR_UNRESPONSIVE_NODE, FORECAST_BACKOFF_MINUTES);
    }

    /**
     * Get Forecaster config object if present
     * @param forecasterID forecaster Id
     * @return the forecaster config object or empty Optional
     */
    @Override
    public Optional<Forecaster> getConfigIfPresent(String forecasterID) {
        ForecastNodeState state = states.get(forecasterID);
        return Optional.ofNullable(state).map(ForecastNodeState::getForecasterDef);
    }

    @Override
    public void getConfig(String forecasterID, ActionListener<Optional<? extends Config>> listener) {
        ForecastNodeState state = states.get(forecasterID);
        if (state != null && state.getForecasterDef() != null) {
            listener.onResponse(Optional.of(state.getForecasterDef()));
        } else {
            GetRequest request = new GetRequest(CommonName.CONFIG_INDEX, forecasterID);
            clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetForecasterResponse(forecasterID, listener));
        }
    }

    private ActionListener<GetResponse> onGetForecasterResponse(String forecasterID, ActionListener<Optional<? extends Config>> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Optional.empty());
                return;
            }

            String xc = response.getSourceAsString();
            LOG.debug("Fetched forecaster: {}", xc);

            try (
                XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, xc)
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Forecaster forecaster = Forecaster.parse(parser, response.getId());
                // end execution if all features are disabled
                if (forecaster.getEnabledFeatureIds().isEmpty()) {
                    listener
                        .onFailure(
                            new EndRunException(forecasterID, CommonMessages.ALL_FEATURES_DISABLED_ERR_MSG, true).countedInStats(false)
                        );
                    return;
                }
                ForecastNodeState state = states.computeIfAbsent(forecasterID, id -> new ForecastNodeState(id, clock));
                state.setForecasterDef(forecaster);

                listener.onResponse(Optional.of(forecaster));
            } catch (Exception t) {
                LOG.error("Fail to parse forecaster {}", forecasterID);
                LOG.error("Stack trace:", t);
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure);
    }

    /**
     * Clean states if it is older than our stateTtl. transportState has to be a
     * ConcurrentHashMap otherwise we will have
     * java.util.ConcurrentModificationException.
     *
     */
    @Override
    public void maintenance() {
        maintenance(states, stateTtl);
    }

    /**
     * Used in delete workflow
     *
     * @param forecasterId forecaster ID
     */
    @Override
    public void clear(String forecasterId) {
        states.remove(forecasterId);
    }

    /**
     * Get a forecaster's exception.  The method has side effect.
     * We reset error after calling the method because
     * 1) We record a forecaster's exception in each interval.  There is no need
     *  to record it twice.
     * 2) EndRunExceptions can stop job running. We only want to send the same
     *  signal once for each exception.
     * @param forecasterId forecaster id
     * @return the forecaster's exception
     */
    @Override
    public Optional<Exception> fetchExceptionAndClear(String forecasterId) {
        ForecastNodeState state = states.get(forecasterId);
        if (state == null) {
            return Optional.empty();
        }

        Optional<Exception> exception = state.getException();
        exception.ifPresent(e -> state.setException(null));
        return exception;
    }

    /**
     * For single-stream forecaster, we have one exception per interval.  When
     * an interval starts, it fetches and clears the exception.
     * For HC forecaster, there can be one exception per entity.  To not bloat memory
     * with exceptions, we will keep only one exception. An exception has 3 purposes:
     * 1) stop detector if nothing else works;
     * 2) increment error stats to ticket about high-error domain
     * 3) debugging.
     *
     * For HC forecaster, we record all entities' exceptions in forecast results. So 3)
     * is covered.  As long as we keep one exception among all exceptions, 2)
     * is covered.  So the only thing we have to pay attention is to keep EndRunException.
     * When overriding an exception, EndRunException has priority.
     * @param forecasterId Forecaster Id
     * @param e Exception to set
     */
    @Override
    public void setException(String forecasterId, Exception e) {
        if (e == null || Strings.isEmpty(forecasterId)) {
            return;
        }
        ForecastNodeState state = states.computeIfAbsent(forecasterId, d -> new ForecastNodeState(forecasterId, clock));
        Optional<Exception> exception = state.getException();
        if (exception.isPresent()) {
            Exception higherPriorityException = ExceptionUtil.selectHigherPriorityException(e, exception.get());
            if (higherPriorityException != e) {
                return;
            }
        }

        state.setException(e);
    }
}
