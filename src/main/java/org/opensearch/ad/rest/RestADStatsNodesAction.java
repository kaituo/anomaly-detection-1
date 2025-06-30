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

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI;

import java.util.List;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.rest.RestStatsAction;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * Internal REST handler for AD node stats used by the HTTP node communicator.
 */
public class RestADStatsNodesAction extends RestStatsAction {

    private static final String STATS_NODES_ACTION = "stats_anomaly_detector_nodes";
    private static final String STATS_NODES_PATH = AD_BASE_INTERNAL_DETECTORS_URI + "/" + RestHandlerUtils.STATS_NODES;
    private final Settings settings;
    private final ClusterService clusterService;

    public RestADStatsNodesAction(ADStats timeSeriesStats, DiscoveryNodeSelector nodeFilter, Settings settings) {
        this(timeSeriesStats, nodeFilter, settings, null);
    }

    public RestADStatsNodesAction(
        ADStats timeSeriesStats,
        DiscoveryNodeSelector nodeFilter,
        Settings settings,
        ClusterService clusterService
    ) {
        super(timeSeriesStats, nodeFilter);
        this.settings = settings;
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return STATS_NODES_ACTION;
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }
        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(this.settings), request);
        StatsRequest parsedRequest = getRequest(request, tenantId);
        StatsRequest statsRequest = parsedRequest;
        if (clusterService != null && clusterService.localNode() != null) {
            statsRequest = new StatsRequest(tenantId, clusterService.localNode());
            statsRequest.addAll(parsedRequest.getStatsToBeRetrieved());
        }
        StatsRequest finalStatsRequest = statsRequest;
        return channel -> client.execute(ADStatsNodesAction.INSTANCE, finalStatsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(new Route(RestRequest.Method.GET, STATS_NODES_PATH), new Route(RestRequest.Method.GET, STATS_NODES_PATH + "/{stat}"));
    }
}
