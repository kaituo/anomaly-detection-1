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

import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_STOP_FORECASTER;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.inject.Inject;
import org.opensearch.forecast.client.ForecastNodeCommunicator;
import org.opensearch.timeseries.transport.AbstractStopConfigTransportAction;
import org.opensearch.transport.TransportService;

public class StopForecasterTransportAction extends AbstractStopConfigTransportAction {

    private static final Logger LOG = LogManager.getLogger(StopForecasterTransportAction.class);

    @Inject
    public StopForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ForecastNodeCommunicator nodeCommunicator
    ) {
        super(
            StopForecasterAction.NAME,
            transportService,
            actionFilters,
            nodeCommunicator,
            LOG,
            FAIL_TO_STOP_FORECASTER,
            "Cannot delete all models of forecaster {}",
            "models of forecaster {} get deleted",
            "Deletion of forecaster [{}] has exception."
        );
    }
}
