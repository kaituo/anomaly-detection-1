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

package org.opensearch.ad.transport.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Local host call only. Safe in multitenant.")
public class ADIndexMemoryPressureAwareResultHandler extends
    IndexMemoryPressureAwareResultHandler<AnomalyResult, ADResultWriteRequest, ADResultBulkRequest, ResultBulkResponse, ADIndex, ADDelegatingDataManagement> {
    private static final Logger LOG = LogManager.getLogger(ADIndexMemoryPressureAwareResultHandler.class);

    @Inject
    public ADIndexMemoryPressureAwareResultHandler(
        Client client,
        ADDelegatingDataManagement anomalyDetectionIndices,
        DiscoveryNodeSelector discoveryNodeSelector
    ) {
        super(client, anomalyDetectionIndices, discoveryNodeSelector);
    }

    @Override
    protected void bulk(ADResultBulkRequest currentBulkRequest, ActionListener<ResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new TimeSeriesException("no result to save"));
            return;
        }
        // we retry failed bulk requests in ResultWriteWorker
        client.execute(ADResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<ResultBulkResponse>wrap(response -> {
            LOG.debug(CommonMessages.SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
