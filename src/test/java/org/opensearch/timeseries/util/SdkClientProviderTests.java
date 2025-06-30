/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.Mockito.mock;

import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

public class SdkClientProviderTests extends OpenSearchTestCase {

    public void testBuildSdkClientReturnsNullWhenMultiTenancyDisabled() {
        SdkClient sdkClient = SdkClientProvider
            .buildSdkClient(
                mock(Client.class),
                NamedXContentRegistry.EMPTY,
                "thread-pool",
                false,
                Settings.EMPTY,
                AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
                AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
                mock(Logger.class)
            );

        assertNull(sdkClient);
    }

    public void testBuildSdkClientReturnsNullWhenRemoteMetadataSettingsAreMissing() {
        Settings settings = Settings.builder().put(TimeSeriesSettings.REGION.getKey(), "us-west-2").build();

        SdkClient sdkClient = SdkClientProvider
            .buildSdkClient(
                mock(Client.class),
                NamedXContentRegistry.EMPTY,
                "thread-pool",
                true,
                settings,
                AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
                AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
                mock(Logger.class)
            );

        assertNull(sdkClient);
    }
}
