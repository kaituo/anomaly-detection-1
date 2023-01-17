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

import org.opensearch.action.ActionType;
import org.opensearch.forecast.constant.ForecastCommonValue;
import org.opensearch.timeseries.transport.StatsNodesResponse;

/**
 * ADStatsNodesAction class
 */
public class ForecastStatsNodesAction extends ActionType<StatsNodesResponse> {

    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.INTERNAL_ACTION_PREFIX + "stats/nodes";
    public static final ForecastStatsNodesAction INSTANCE = new ForecastStatsNodesAction();

    /**
     * Constructor
     */
    private ForecastStatsNodesAction() {
        super(NAME, StatsNodesResponse::new);
    }

}
