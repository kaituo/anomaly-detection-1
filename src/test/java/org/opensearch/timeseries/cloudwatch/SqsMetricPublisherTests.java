/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import java.util.List;

import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class SqsMetricPublisherTests extends OpenSearchTestCase {

    public void testQueueNamesIncludePrimaryAndDistinctExtras() {
        assertEquals(
            List.of("ad-jobs.fifo", "ad-jobs-overflow.fifo", "ad-jobs-retry.fifo"),
            SqsMetricPublisher.queueNames(" ad-jobs.fifo ", List.of("ad-jobs-overflow.fifo", "", "ad-jobs.fifo", " ad-jobs-retry.fifo "))
        );
    }

    public void testPublisherIsManagedLifecycleComponent() {
        assertTrue(LifecycleComponent.class.isAssignableFrom(SqsMetricPublisher.class));
    }

    public void testCoordinatorSqsMetricServiceNameUsesOnepodCoordinator() {
        Settings settings = Settings.builder().put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "master-onepod").build();

        assertEquals("coordinator-onepod", SqsMetricPublisher.coordinatorSqsMetricServiceName(settings));
    }

    public void testCoordinatorSqsMetricServiceNameUsesFleetCoordinator() {
        Settings settings = Settings.builder().put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "master-fleet").build();

        assertEquals("coordinator-fleet", SqsMetricPublisher.coordinatorSqsMetricServiceName(settings));
    }

    public void testCoordinatorSqsMetricServiceNameUsesOnepodFromList() {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "master-onepod,coordinator-onepod,model-onepod")
            .build();

        assertEquals("coordinator-onepod", SqsMetricPublisher.coordinatorSqsMetricServiceName(settings));
    }

    public void testCoordinatorSqsMetricServiceNameUsesFleetFromList() {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.getKey(), "master-fleet,coordinator-fleet,model-fleet")
            .build();

        assertEquals("coordinator-fleet", SqsMetricPublisher.coordinatorSqsMetricServiceName(settings));
    }

    public void testCoordinatorSqsMetricServiceNameKeepsBlankSettingBlank() {
        assertEquals("", SqsMetricPublisher.coordinatorSqsMetricServiceName(Settings.EMPTY));
    }
}
