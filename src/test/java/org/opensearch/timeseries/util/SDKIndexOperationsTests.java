/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.AbstractTimeSeriesTest;

public class SDKIndexOperationsTests extends AbstractTimeSeriesTest {

    public void testAossDataPlaneSkipsClusterHealthApi() {
        AtomicInteger clientCalls = new AtomicInteger();
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss").build();
        SDKIndexOperations indexOperations = new SDKIndexOperations(settings, (tenantId, dataSourceId) -> {
            clientCalls.incrementAndGet();
            return mock(RestClient.class);
        });

        assertEquals("green", indexOperations.getIndexHealthStatus("tenant", ADCommonName.ANOMALY_RESULT_INDEX_ALIAS));
        assertEquals(0, clientCalls.get());
    }
}
