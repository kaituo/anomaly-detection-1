/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.remote.metadata.client.SdkClient;

/**
 * Factory for repository-specific {@link ConfigDocumentStore} implementations.
 */
public interface ConfigDocumentStoreFactory {
    ConfigDocumentStore create(SdkClient sdkClient, Settings settings, ClusterService clusterService);
}
