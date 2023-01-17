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

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.ADCommonValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkResponse;
import org.opensearch.transport.TransportRequestOptions;

public class ADResultBulkAction extends ActionType<TimeSeriesResultBulkResponse> {

    // Internal Action which is not used for public facing RestAPIs.
    public static final String NAME = ADCommonValue.INTERNAL_ACTION_PREFIX + "write/bulk";
    public static final ADResultBulkAction INSTANCE = new ADResultBulkAction();

    private ADResultBulkAction() {
        super(NAME, TimeSeriesResultBulkResponse::new);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK).build();
    }
}
