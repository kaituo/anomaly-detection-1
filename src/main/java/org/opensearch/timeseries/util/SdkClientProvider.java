/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_AWARE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

/**
 * Shared helper for building remote metadata {@link SdkClient} instances.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Client required by remote SDK library.")
public final class SdkClientProvider {

    private SdkClientProvider() {}

    public static SdkClient buildSdkClient(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String threadPoolName,
        boolean multiTenancyEnabled,
        Settings settings,
        Setting<String> remoteMetadataEndpoint,
        Setting<String> remoteMetadataServiceName,
        Logger logger
    ) {
        if (multiTenancyEnabled == false) {
            return null;
        }

        String endpoint = remoteMetadataEndpoint.get(settings);
        String serviceName = remoteMetadataServiceName.get(settings);
        String region = TimeSeriesSettings.REGION.get(settings);

        List<String> missingSettings = new ArrayList<>();
        if (Strings.isBlank(endpoint)) {
            missingSettings.add(remoteMetadataEndpoint.getKey());
        }
        if (Strings.isBlank(serviceName)) {
            missingSettings.add(remoteMetadataServiceName.getKey());
        }
        if (Strings.isBlank(region)) {
            missingSettings.add(TimeSeriesSettings.REGION.getKey());
        }

        if (missingSettings.isEmpty() == false) {
            if (logger != null) {
                logger.warn("Remote metadata settings [{}] are not fully configured.", missingSettings);
            }
            return null;
        }

        Map<String, String> sdkClientSettings = Map
            .ofEntries(
                Map.entry(REMOTE_METADATA_TYPE_KEY, "AWSDynamoDB"),
                Map.entry(REMOTE_METADATA_ENDPOINT_KEY, endpoint),
                Map.entry(REMOTE_METADATA_REGION_KEY, region),
                Map.entry(REMOTE_METADATA_SERVICE_NAME_KEY, serviceName),
                Map.entry(TENANT_AWARE_KEY, "true"),
                Map.entry(TENANT_ID_FIELD_KEY, CommonName.TENANT_ID_FIELD)
            );

        return SdkClientFactory.createSdkClient(client, xContentRegistry, sdkClientSettings, client.threadPool().executor(threadPoolName));
    }
}
