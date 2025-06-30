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

package org.opensearch.forecast.rest.handler;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.IndexForecasterResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.transport.TransportService;

/**
 * process create/update forecaster request
 *
 */
public class IndexForecasterActionHandler extends AbstractForecasterActionHandler<IndexForecasterResponse> {
    /**
     * Constructor function.
     *
     * @param clusterService             ClusterService
     * @param transportService           OS transport service
     * @param forecastIndices            forecast index manager
     * @param forecasterId               forecaster identifier
     * @param seqNo                      sequence number of last modification
     * @param primaryTerm                primary term of last modification
     * @param refreshPolicy              refresh policy
     * @param forecaster                 forecaster instance
     * @param requestTimeout             request time out configuration
     * @param maxSingleStreamForecasters max single-stream forecasters allowed
     * @param maxHCForecasters           max HC forecasters allowed
     * @param maxForecastFeatures        max features allowed per forecaster
     * @param maxCategoricalFields       max number of categorical fields
     * @param method                     Rest Method type
     * @param xContentRegistry           Registry which is used for XContentParser
     * @param user                       User context
     * @param taskManager                Forecast task manager
     * @param searchFeatureDao           Search utility
     * @param settings                   Node settings
     * @param configStore                Config store
     */
    public IndexForecasterActionHandler(
        ClusterService clusterService,
        DataAccess dataAccess,
        TransportService transportService,
        ForecastDelegatingDataManagement forecastIndices,
        String forecasterId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        Forecaster forecaster,
        TimeValue requestTimeout,
        Integer maxSingleStreamForecasters,
        Integer maxHCForecasters,
        Integer maxForecastFeatures,
        Integer maxCategoricalFields,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ForecastTaskManager taskManager,
        SearchFeatureDao searchFeatureDao,
        Settings settings,
        RunContext runContext
    ) {
        super(
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
            user,
            taskManager,
            searchFeatureDao,
            null,
            false,
            null,
            settings,
            runContext
        );
    }
}
