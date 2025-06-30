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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_STOP_DETECTOR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.common.inject.Inject;
import org.opensearch.timeseries.transport.AbstractStopConfigTransportAction;
import org.opensearch.transport.TransportService;

public class StopDetectorTransportAction extends AbstractStopConfigTransportAction {

    private static final Logger LOG = LogManager.getLogger(StopDetectorTransportAction.class);

    @Inject
    public StopDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADNodeCommunicator nodeCommunicator
    ) {
        super(
            StopDetectorAction.NAME,
            transportService,
            actionFilters,
            nodeCommunicator,
            LOG,
            FAIL_TO_STOP_DETECTOR,
            "Cannot delete all models of detector {}",
            "models of detector {} get deleted",
            "Deletion of detector [{}] has exception."
        );
    }
}
