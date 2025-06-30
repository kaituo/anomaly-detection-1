/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

/**
 * Factory for repository-specific {@link ConfigDocumentStore} implementations.
 */
public interface ConfigDocumentStoreFactory {
    ConfigDocumentStore create(Settings settings, ClusterService clusterService);
}
