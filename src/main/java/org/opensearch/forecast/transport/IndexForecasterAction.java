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

public class IndexForecasterAction extends ActionType<IndexForecasterResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = ForecastCommonValue.EXTERNAL_ACTION_PREFIX + "forecaster/write";
    public static final IndexForecasterAction INSTANCE = new IndexForecasterAction();

    private IndexForecasterAction() {
        super(NAME, IndexForecasterResponse::new);
    }
}
