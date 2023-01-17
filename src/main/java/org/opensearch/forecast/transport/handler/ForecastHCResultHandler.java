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

package org.opensearch.forecast.transport.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.transport.ForecastResultBulkAction;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexPressureAwareResultHandler;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.IndexUtils;

public class ForecastHCResultHandler extends IndexPressureAwareResultHandler<ForecastResult, ForecastResultBulkRequest, TimeSeriesResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ForecastHCResultHandler.class);

    @Inject
    public ForecastHCResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        ForecastIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            ForecastIndex.RESULT.getIndexName(),
            anomalyDetectionIndices,
            clientUtil,
            indexUtils,
            clusterService,
            ForecastSettings.FORECAST_BACKOFF_INITIAL_DELAY,
            ForecastSettings.FORECAST_MAX_RETRY_FOR_BACKOFF

        );
    }

    @Override
    public void bulk(ForecastResultBulkRequest currentBulkRequest, ActionListener<TimeSeriesResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new TimeSeriesException("no result to save"));
            return;
        }
        client.execute(ForecastResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<TimeSeriesResultBulkResponse>wrap(response -> {
            LOG.debug(CommonMessages.SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
