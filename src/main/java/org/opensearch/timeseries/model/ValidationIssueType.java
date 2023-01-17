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

package org.opensearch.timeseries.model;

import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.Name;

public enum ValidationIssueType implements Name {
    NAME(Config.NAME_FIELD),
    TIMEFIELD_FIELD(Config.TIMEFIELD_FIELD),
    SHINGLE_SIZE_FIELD(Config.SHINGLE_SIZE_FIELD),
    INDICES(Config.INDICES_FIELD),
    FEATURE_ATTRIBUTES(Config.FEATURE_ATTRIBUTES_FIELD),
    CATEGORY(Config.CATEGORY_FIELD),
    FILTER_QUERY(Config.FILTER_QUERY_FIELD),
    WINDOW_DELAY(Config.WINDOW_DELAY_FIELD),
    GENERAL_SETTINGS(Config.GENERAL_SETTINGS),
    RESULT_INDEX(Config.RESULT_INDEX_FIELD),
    TIMEOUT(Config.TIMEOUT),
    AGGREGATION(Config.AGGREGATION), // this is a unique case where aggregation failed due to an issue in core but
                                              // don't want to throw exception
    INTERVAL(Config.INTERVAL_FIELD),

    IMPUTATION(Config.IMPUTATION_OPTION_FIELD),

    HORIZON_SIZE(Forecaster.HORIZON_FIELD);

    private String name;

    ValidationIssueType(String name) {
        this.name = name;
    }

    /**
     * Get validation type
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }
}
