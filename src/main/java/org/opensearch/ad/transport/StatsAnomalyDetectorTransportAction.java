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

package org.opensearch.ad.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.BaseStatsTransportAction;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.transport.StatsResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;

public class StatsAnomalyDetectorTransportAction extends BaseStatsTransportAction {
    public static final String DETECTOR_TYPE_AGG = "detector_type_agg";
    static final String PUBLIC_NODE_ID_PREFIX = "node-";
    private final ADDelegatingDataManagement adDataManagement;
    private final ADNodeCommunicator nodeCommunicator;
    private final boolean redactNodeIds;

    @Inject
    public StatsAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADStats adStats,
        ClusterService clusterService,
        Settings settings,
        ADDelegatingDataManagement adDataManagement,
        DataAccess dataAccess,
        RunContext runContext,
        ADNodeCommunicator nodeCommunicator
    ) {
        super(transportService, actionFilters, adStats, clusterService, StatsAnomalyDetectorAction.NAME, dataAccess, runContext);
        this.adDataManagement = adDataManagement;
        this.nodeCommunicator = nodeCommunicator;
        this.redactNodeIds = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings);
    }

    /**
     * Make async request to get the number of detectors in AnomalyDetector.ANOMALY_DETECTORS_INDEX if necessary
     * and, onResponse, gather the cluster statistics
     *
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    @Override
    protected void getClusterStats(MultiResponsesDelegateActionListener<StatsResponse> listener, StatsRequest adStatsRequest) {
        StatsResponse adStatsResponse = new StatsResponse();
        if ((adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HC_DETECTOR_COUNT.getName()))
            && adDataManagement.doesConfigIndexExist()) {

            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(DETECTOR_TYPE_AGG).field(AnomalyDetector.DETECTOR_TYPE_FIELD);
            SearchRequest request = new SearchRequest()
                .indices(ADCommonName.CONFIG_INDEX)
                .source(new SearchSourceBuilder().aggregation(termsAgg).size(0).trackTotalHits(true));

            dataAccess.search(request, TenantContext.user(adStatsRequest.getTenantId()), ActionListener.wrap(r -> {
                Terms aggregation = r.getAggregations().get(DETECTOR_TYPE_AGG);
                List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
                long totalDetectors = r.getHits().getTotalHits().value();
                long totalSingleEntityDetectors = 0;
                long totalMultiEntityDetectors = 0;
                for (Terms.Bucket b : buckets) {
                    if (AnomalyDetectorType.SINGLE_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.REALTIME_SINGLE_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name().equals(b.getKeyAsString())) {
                        totalSingleEntityDetectors += b.getDocCount();
                    }
                    if (AnomalyDetectorType.MULTI_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.REALTIME_MULTI_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.HISTORICAL_MULTI_ENTITY.name().equals(b.getKeyAsString())) {
                        totalMultiEntityDetectors += b.getDocCount();
                    }
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())) {
                    stats.getStat(StatNames.DETECTOR_COUNT.getName()).setValueForTenant(adStatsRequest.getTenantId(), totalDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName())) {
                    stats
                        .getStat(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName())
                        .setValueForTenant(adStatsRequest.getTenantId(), totalSingleEntityDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HC_DETECTOR_COUNT.getName())) {
                    stats
                        .getStat(StatNames.HC_DETECTOR_COUNT.getName())
                        .setValueForTenant(adStatsRequest.getTenantId(), totalMultiEntityDetectors);
                }
                adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
                listener.onResponse(adStatsResponse);
            }, e -> listener.onFailure(e)));
        } else {
            adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
            listener.onResponse(adStatsResponse);
        }
    }

    /**
     * Make async request to get the Anomaly Detection statistics from each node and, onResponse, set the
     * ADStatsNodesResponse field of ADStatsResponse
     *
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    @Override
    protected void getNodeStats(MultiResponsesDelegateActionListener<StatsResponse> listener, StatsRequest adStatsRequest) {
        nodeCommunicator.stat(adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            StatsResponse restADStatsResponse = new StatsResponse();
            StatsNodesResponse statsNodesResponse = mergeLocalNodeStats(adStatsRequest, adStatsResponse);
            restADStatsResponse.setStatsNodesResponse(redactNodeIds ? redactNodeIds(statsNodesResponse) : statsNodesResponse);
            listener.onResponse(restADStatsResponse);
        }, listener::onFailure));
    }

    static StatsNodesResponse redactNodeIds(StatsNodesResponse statsResponse) {
        List<StatsNodeResponse> redactedResponses = new ArrayList<>();
        int nodeNumber = 1;
        for (StatsNodeResponse response : statsResponse.getNodes()) {
            redactedResponses.add(new StatsNodeResponse(createPublicNode(PUBLIC_NODE_ID_PREFIX + nodeNumber), response.getStatsMap()));
            nodeNumber++;
        }
        return new StatsNodesResponse(statsResponse.getClusterName(), redactedResponses, statsResponse.failures());
    }

    private static DiscoveryNode createPublicNode(String nodeId) {
        return new DiscoveryNode(nodeId, new TransportAddress(TransportAddress.META_ADDRESS, 0), Version.CURRENT);
    }

    private StatsNodesResponse mergeLocalNodeStats(StatsRequest request, StatsNodesResponse remoteStatsResponse) {
        Map<String, Object> localNodeStats = getNodeStatsMap(request);
        if (localNodeStats.isEmpty() || !shouldIncludeLocalNode(request) || localNodeAlreadyPresent(remoteStatsResponse)) {
            return remoteStatsResponse;
        }

        List<StatsNodeResponse> mergedResponses = new ArrayList<>(remoteStatsResponse.getNodes());
        mergedResponses.add(new StatsNodeResponse(clusterService.localNode(), localNodeStats));
        return new StatsNodesResponse(remoteStatsResponse.getClusterName(), mergedResponses, remoteStatsResponse.failures());
    }

    private boolean shouldIncludeLocalNode(StatsRequest request) {
        if (clusterService.localNode() == null) {
            return false;
        }

        String[] requestedNodeIds = request.nodesIds();
        if (requestedNodeIds == null || requestedNodeIds.length == 0) {
            return true;
        }

        Set<String> requestedNodes = new HashSet<>(Arrays.asList(requestedNodeIds));
        return requestedNodes.contains("_all")
            || requestedNodes.contains(clusterService.localNode().getId())
            || requestedNodes.contains(clusterService.localNode().getName());
    }

    private boolean localNodeAlreadyPresent(StatsNodesResponse statsResponse) {
        if (clusterService.localNode() == null) {
            return true;
        }

        return statsResponse
            .getNodes()
            .stream()
            .map(StatsNodeResponse::getNode)
            .anyMatch(node -> node != null && clusterService.localNode().getId().equals(node.getId()));
    }
}
