/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

public enum AnalysisType {
    AD,
    FORECAST,
    HOURLY_MAINTENANCE,
    DAILY_S3_CHECKPOINT_CLEANUP,
    // for test
    UNKNOWN;

    public boolean isForecast() {
        return this == FORECAST;
    }

    public boolean isAD() {
        return this == AD;
    }
}
