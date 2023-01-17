/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.task;

import org.opensearch.action.ActionListener;
import org.opensearch.timeseries.model.Config;
import org.opensearch.transport.TransportService;

public interface RealTimeTaskManager {
    /**
     *  the function initializes the cache and only performs cleanup if it is deemed necessary.
     * @param id config id
     * @param config config accessor
     * @param transportService Transport service
     * @param listener listener to return back init success or not
     */
    public void initCacheWithCleanupIfRequired(
        String id,
        Config config,
        TransportService transportService,
        ActionListener<Boolean> listener
    );
}
