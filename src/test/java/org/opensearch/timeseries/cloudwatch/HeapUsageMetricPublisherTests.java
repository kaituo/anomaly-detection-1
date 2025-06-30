/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.test.OpenSearchTestCase;

public class HeapUsageMetricPublisherTests extends OpenSearchTestCase {

    public void testPublisherIsManagedLifecycleComponent() {
        assertTrue(LifecycleComponent.class.isAssignableFrom(HeapUsageMetricPublisher.class));
    }
}
