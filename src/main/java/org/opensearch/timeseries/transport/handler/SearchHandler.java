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

package org.opensearch.timeseries.transport.handler;

import static org.opensearch.timeseries.util.ParseUtils.isAdmin;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.PluginClient;

/**
 * Handle general search request, check user role and return search response.
 */
@SuppressForbidden(reason = "PluginClient is only used for single-tenant resource authz; multi-tenant-safe search goes through DataAccess.")
public class SearchHandler {
    private final Logger logger = LogManager.getLogger(SearchHandler.class);
    private final PluginClient pluginClient;
    private final DataAccess searcher;
    private final RunContext runContext;
    private volatile Boolean filterEnabled;

    public SearchHandler(
        Settings settings,
        ClusterService clusterService,
        PluginClient pluginClient,
        DataAccess searcher,
        Setting<Boolean> filterByBackendRoleSetting,
        RunContext runContext
    ) {
        this.pluginClient = pluginClient;
        this.searcher = searcher;
        this.runContext = runContext;
        filterEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterEnabled = it);
    }

    /**
     * Validate user role, add backend role filter if filter enabled
     * and execute search.
     *
     * @param request        search request
     * @param resourceType
     * @param actionListener action listener
     */
    public void search(SearchRequest request, String resourceType, ActionListener<SearchResponse> actionListener) {
        User user = runContext.getUser();
        boolean shouldUseResourceAuthz = ParseUtils.shouldUseResourceAuthz();
        ActionListener<SearchResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_SEARCH);
        runContext.runWithSystemAuth(() -> {
            if (pluginClient != null && shouldUseResourceAuthz) {
                // request will be auto-filtered in security plugin
                // TODO: we don't support resource authz for multi-tenant as it requires security plugin
                // to centralize the authN logic (e.g., verify access to resource). In a multi-tenant AD,
                // there is no security plugin. To support that, we need to either to move some of the
                // authN logic to AD plugin and make security plugin to support external
                // access resource sharing calls from outside the cluster.
                pluginClient.search(request, actionListener);
            } else {
                validateRole(request, user, listener);
            }
        }, exception -> {
            logger.error(exception);
            listener.onFailure(exception);
        });
    }

    private void validateRole(SearchRequest request, User user, ActionListener<SearchResponse> listener) {
        // We used the SearchRequest preference field to convey a tenant id if any
        String tenantId = null;
        if (request.preference() != null) {
            tenantId = request.preference();
            request.preference(null);
        }
        if (user == null || !filterEnabled || isAdmin(user)) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            // case 3: user is admin which means we don't have to check backend role filtering
            searcher.search(request, TenantContext.user(tenantId), listener);
        } else {
            // Security is enabled, filter is enabled and user isn't admin
            try {
                ParseUtils.addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                searcher.search(request, TenantContext.user(tenantId), listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

}
