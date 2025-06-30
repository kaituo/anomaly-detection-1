/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

public class SqsMetricPublisherTests extends OpenSearchTestCase {

    public void testQueueNamesIncludePrimaryAndDistinctExtras() {
        assertEquals(
            List.of("ad-jobs.fifo", "ad-jobs-overflow.fifo", "ad-jobs-retry.fifo"),
            SqsMetricPublisher.queueNames(" ad-jobs.fifo ", List.of("ad-jobs-overflow.fifo", "", "ad-jobs.fifo", " ad-jobs-retry.fifo "))
        );
    }
}
