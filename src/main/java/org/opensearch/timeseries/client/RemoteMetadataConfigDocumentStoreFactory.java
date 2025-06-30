/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.threadpool.ThreadPool;

/**
 * Default factory that builds the remote metadata-backed config store.
 */
public class RemoteMetadataConfigDocumentStoreFactory implements ConfigDocumentStoreFactory {
    private final SdkClient sdkClient;

    public RemoteMetadataConfigDocumentStoreFactory(SdkClient sdkClient) {
        this.sdkClient = Objects.requireNonNull(sdkClient, "sdkClient must not be null");
    }

    @Override
    public ConfigDocumentStore create(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        return new RemoteMetadataConfigDocumentStore(sdkClient);
    }
}
