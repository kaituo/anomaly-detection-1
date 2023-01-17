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

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.transport.IndexForecasterResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

public abstract class AbstractForecasterActionHandler<T extends ActionResponse> extends
    AbstractTimeSeriesActionHandler<T, ForecastIndex, ForecastIndexManagement> {
    protected final Logger logger = LogManager.getLogger(AbstractForecasterActionHandler.class);

    public static final String EXCEEDED_MAX_HC_FORECASTERS_PREFIX_MSG = "Can't create more than %d HC forecasters.";
    public static final String EXCEEDED_MAX_SINGLE_STREAM_FORECASTERS_PREFIX_MSG = "Can't create more than %d single-stream forecasters.";
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create forecasters as no document is found in the indices: ";
    public static final String DUPLICATE_FORECASTER_MSG = "Cannot create forecasters with name [%s] as it's already used by forecaster %s";
    public static final String VALIDATION_FEATURE_FAILURE = "Validation failed for feature(s) of forecaster %s";

    protected final Integer maxSingleStreamForecasters;
    protected final Integer maxHCForecasters;

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param clientUtil              Forecast security client
     * @param transportService        ES transport service
     * @param forecastIndices         forecast index manager
     * @param forecasterId            forecaster identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param forecaster              forecaster instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleStreamForecasters     max single-stream forecasters allowed
     * @param maxHCForecasters        max HC forecasters allowed
     * @param maxFeatures             max features allowed per forecaster
     * @param maxCategoricalFields    max categorical fields allowed
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param clock                   clock object to know when to timeout
     * @param isDryRun                Whether handler is dryrun or not
     */
    public AbstractForecasterActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        TransportService transportService,
        ForecastIndexManagement forecastIndices,
        String forecasterId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        Forecaster forecaster,
        TimeValue requestTimeout,
        Integer maxSingleStreamForecasters,
        Integer maxHCForecasters,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        boolean isDryRun
    ) {
        super(
            forecaster,
            forecastIndices,
            isDryRun,
            client,
            forecasterId,
            clientUtil,
            user,
            method,
            clusterService,
            xContentRegistry,
            transportService,
            requestTimeout,
            refreshPolicy,
            seqNo,
            primaryTerm,
            validationType,
            searchFeatureDao,
            maxFeatures,
            maxCategoricalFields,
            AnalysisType.FORECAST
        );
        this.maxSingleStreamForecasters = maxSingleStreamForecasters;
        this.maxHCForecasters = maxHCForecasters;
    }

    @Override
    protected TimeSeriesException createValidationException(String msg, ValidationIssueType type) {
        return new ValidationException(msg, type, ValidationAspect.FORECASTER);
    }

    @Override
    protected Forecaster parse(XContentParser parser, GetResponse response) throws IOException {
        return Forecaster.parse(parser, response.getId(), response.getVersion());
    }

    // TODO: add method body once backtesting implementation is ready
    @Override
    protected void confirmHistoricalRunning(String id, ActionListener<Void> listener) {

    }

    @Override
    protected String getExceedMaxSingleStreamConfigsErrorMsg(int maxSingleStreamConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_SINGLE_STREAM_FORECASTERS_PREFIX_MSG, getMaxSingleStreamConfigs());
    }

    @Override
    protected String getExceedMaxHCConfigsErrorMsg(int maxHCConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_HC_FORECASTERS_PREFIX_MSG, getMaxHCConfigs());
    }

    @Override
    protected String getNoDocsInUserIndexErrorMsg(String suppliedIndices) {
        return String.format(Locale.ROOT, NO_DOCS_IN_USER_INDEX_MSG, suppliedIndices);
    }

    @Override
    protected String getDuplicateConfigErrorMsg(String name, List<String> otherConfigIds) {
        return String.format(Locale.ROOT, DUPLICATE_FORECASTER_MSG, name, otherConfigIds);
    }

    @Override
    protected Config copyConfig(User user, Config config) {
        return new Forecaster(
            config.getId(),
            config.getVersion(),
            config.getName(),
            config.getDescription(),
            config.getTimeField(),
            config.getIndices(),
            config.getFeatureAttributes(),
            config.getFilterQuery(),
            config.getInterval(),
            config.getWindowDelay(),
            config.getShingleSize(),
            config.getUiMetadata(),
            config.getSchemaVersion(),
            Instant.now(),
            config.getCategoryFields(),
            user,
            config.getCustomResultIndex(),
            ((Forecaster) config).getHorizon(),
            config.getImputationOption()
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T createIndexConfigResponse(IndexResponse indexResponse, Config config) {
        return (T) new IndexForecasterResponse(
            indexResponse.getId(),
            indexResponse.getVersion(),
            indexResponse.getSeqNo(),
            indexResponse.getPrimaryTerm(),
            (Forecaster) config,
            RestStatus.CREATED
        );
    }

    @Override
    protected Set<ValidationAspect> getDefaultValidationType() {
        return Sets.newHashSet(ValidationAspect.FORECASTER);
    }

    @Override
    protected String getFeatureErrorMsg(String name) {
        return String.format(Locale.ROOT, VALIDATION_FEATURE_FAILURE, name);
    }

    @Override
    protected Integer getMaxSingleStreamConfigs() {
        return maxSingleStreamForecasters;
    }

    @Override
    protected Integer getMaxHCConfigs() {
        return maxHCForecasters;
    }

    @Override
    protected void validateModel(ActionListener<T> listener) {
        // TODO: add model validation and return with listener
    }
}
