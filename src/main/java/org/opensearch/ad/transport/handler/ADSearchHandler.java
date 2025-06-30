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

package org.opensearch.ad.transport.handler;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.transport.handler.SearchHandler;
import org.opensearch.timeseries.util.PluginClient;

/**
 * Handle general search request, check user role and return search response.
 */
public class ADSearchHandler extends SearchHandler {

    @SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: PluginClient is only used for the single-tenant/resource-authz path; multi-tenant resource sharing is unsupported.")
    public ADSearchHandler(
        Settings settings,
        ClusterService clusterService,
        PluginClient pluginClient,
        DataAccess searcher,
        RunContext runContext
    ) {
        super(settings, clusterService, pluginClient, searcher, AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES, runContext);
    }
}
