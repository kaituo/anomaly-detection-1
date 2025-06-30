/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.remote.metadata.client.SdkClient;

/**
 * Default factory that builds the remote metadata-backed config store.
 */
public class RemoteMetadataConfigDocumentStoreFactory implements ConfigDocumentStoreFactory {
    @Override
    public ConfigDocumentStore create(SdkClient sdkClient, Settings settings, ClusterService clusterService) {
        return new RemoteMetadataConfigDocumentStore(sdkClient);
    }
}
