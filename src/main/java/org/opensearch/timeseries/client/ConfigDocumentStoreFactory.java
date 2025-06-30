/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * Factory for repository-specific {@link ConfigDocumentStore} implementations.
 */
public interface ConfigDocumentStoreFactory {
    /**
     * @param threadPool the node thread pool whose {@link org.opensearch.common.util.concurrent.ThreadContext}
     *                   the store may need to snapshot/restore across async hops (e.g. CompletableFuture
     *                   completions on the fork-join common pool that lose request-thread transients).
     *                   Implementations that don't need it are free to ignore the argument.
     */
    ConfigDocumentStore create(Settings settings, ClusterService clusterService, ThreadPool threadPool);
}
