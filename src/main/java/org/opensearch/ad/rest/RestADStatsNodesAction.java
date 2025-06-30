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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.rest.RestStatsAction;
import org.opensearch.timeseries.stats.InternalStatNames;
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
    private final ADStats adStats;
    private final DiscoveryNodeSelector nodeFilter;
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
        this.adStats = timeSeriesStats;
        this.nodeFilter = nodeFilter;
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
        StatsRequest parsedRequest = getInternalRequest(request, tenantId);
        StatsRequest statsRequest = parsedRequest;
        if (clusterService != null && clusterService.localNode() != null) {
            statsRequest = new StatsRequest(tenantId, clusterService.localNode());
            statsRequest.timeout(request.param("timeout"));
            statsRequest.addAll(parsedRequest.getStatsToBeRetrieved());
        }
        StatsRequest finalStatsRequest = statsRequest;
        return channel -> client.execute(ADStatsNodesAction.INSTANCE, finalStatsRequest, new RestToXContentListener<>(channel));
    }

    private StatsRequest getInternalRequest(RestRequest request, String tenantId) {
        String nodesIdsStr = request.param("nodeId");
        StatsRequest statsRequest;
        if (!Strings.isEmpty(nodesIdsStr)) {
            statsRequest = new StatsRequest(tenantId, nodesIdsStr.split(","));
        } else {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            statsRequest = new StatsRequest(tenantId, dataNodes);
        }
        statsRequest.timeout(request.param("timeout"));

        String statsStr = request.param("stat");
        if (Strings.isEmpty(statsStr)) {
            statsRequest.addAll(adStats.getStats().keySet());
            return statsRequest;
        }

        Set<String> requestedStats = new HashSet<>(Arrays.asList(statsStr.split(",")));
        Set<String> validStats = internalStats();
        if (requestedStats.size() == 1 && requestedStats.contains(StatsRequest.ALL_STATS_KEY)) {
            statsRequest.addAll(validStats);
            return statsRequest;
        }
        if (requestedStats.contains(StatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException(
                "Request " + request.path() + " contains " + StatsRequest.ALL_STATS_KEY + " and individual stats"
            );
        }

        Set<String> invalidStats = new TreeSet<>();
        for (String stat : requestedStats) {
            if (validStats.contains(stat)) {
                statsRequest.addStat(stat);
            } else {
                invalidStats.add(stat);
            }
        }
        if (!invalidStats.isEmpty()) {
            throw new IllegalArgumentException(unrecognized(request, invalidStats, statsRequest.getStatsToBeRetrieved(), "stat"));
        }
        return statsRequest;
    }

    private Set<String> internalStats() {
        Set<String> validStats = new HashSet<>(adStats.getStats().keySet());
        for (InternalStatNames statName : InternalStatNames.values()) {
            validStats.add(statName.getName());
        }
        return validStats;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(new Route(RestRequest.Method.GET, STATS_NODES_PATH), new Route(RestRequest.Method.GET, STATS_NODES_PATH + "/{stat}"));
    }
}
