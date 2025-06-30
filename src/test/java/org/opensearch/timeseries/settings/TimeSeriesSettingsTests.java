/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class TimeSeriesSettingsTests extends OpenSearchTestCase {

    public void testCloudWatchMetricsServiceNameAcceptsSingleValue() {
        Settings settings = Settings.builder().put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "custom-service").build();

        assertEquals("custom-service", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.MODEL_ROLE));
    }

    public void testCloudWatchMetricsServiceNameSelectsRoleFromList() {
        Settings settings = Settings
            .builder()
            .putList(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "run-coordinator", "run-model")
            .build();

        assertEquals("run-model", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.MODEL_ROLE));
        assertEquals("run-coordinator", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.COORDINATOR_ROLE));
    }

    public void testCloudWatchMetricsServiceNameSelectsRoleFromCommaDelimitedValue() {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "run-coordinator,run-model")
            .build();

        assertEquals("run-model", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.MODEL_ROLE));
        assertEquals("run-coordinator", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.COORDINATOR_ROLE));
    }

    public void testCloudWatchMetricsServiceNameReturnsEmptyWhenMultipleValuesDoNotMatchRole() {
        Settings settings = Settings
            .builder()
            .putList(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "service-a", "service-b")
            .build();

        assertEquals("", TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.MODEL_ROLE));
    }
}
