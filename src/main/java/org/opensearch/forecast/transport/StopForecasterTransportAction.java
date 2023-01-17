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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_STOP_DETECTOR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.transport.StopConfigRequest;
import org.opensearch.timeseries.transport.StopConfigResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;

public class StopForecasterTransportAction extends HandledTransportAction<ActionRequest, StopConfigResponse> {

    private static final Logger LOG = LogManager.getLogger(StopForecasterTransportAction.class);

    private final Client client;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public StopForecasterTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        Client client
    ) {
        super(StopForecasterAction.NAME, transportService, actionFilters, StopConfigRequest::new);
        this.client = client;
        this.nodeFilter = nodeFilter;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<StopConfigResponse> listener) {
        StopConfigRequest request = StopConfigRequest.fromActionRequest(actionRequest);
        String adID = request.getConfigID();
        try {
            // TODO:
        } catch (Exception e) {
            LOG.error(FAIL_TO_STOP_DETECTOR + " " + adID, e);
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            listener.onFailure(new InternalFailure(adID, FAIL_TO_STOP_DETECTOR, cause));
        }
    }
}
