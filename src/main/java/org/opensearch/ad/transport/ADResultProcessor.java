/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_PAGE_SIZE;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportService;

public class ADResultProcessor extends
    ResultProcessor<AnomalyResultRequest, AnomalyResult, AnomalyResultResponse, ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADDelegatingDataManagement, ADTaskManager> {
    private static final Logger LOG = LogManager.getLogger(ADResultProcessor.class);
    private final ADNodeCommunicator adNodeCommunicator;

    public ADResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        StateManager nodeStateManager,
        TransportService transportService,
        ADStats timeSeriesStats,
        ADTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        DataAccess dataAccess,
        Class<AnomalyResultResponse> transportResultResponseClazz,
        FeatureManager featureManager,
        DiscoveryNodeSelector discoveryNodeSelector,
        ADNodeCommunicator adNodeCommunicator
    ) {
        super(
            requestTimeoutSetting,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
            ADCommonName.AD_THREAD_POOL_NAME,
            hashRing,
            nodeStateManager,
            transportService,
            timeSeriesStats,
            realTimeTaskManager,
            xContentRegistry,
            dataAccess,
            transportResultResponseClazz,
            featureManager,
            AD_MAX_ENTITIES_PER_QUERY,
            AD_PAGE_SIZE,
            AnalysisType.AD,
            false,
            discoveryNodeSelector,
            adNodeCommunicator
        );
        this.adNodeCommunicator = adNodeCommunicator;
    }

    @Override
    protected AnomalyResultResponse createResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        String taskId
    ) {
        return new AnomalyResultResponse(features, error, rcfTotalUpdates, configInterval, isHC, taskId);
    }

    @Override
    protected void imputeHC(long dataStartTime, long dataEndTime, String configID, String tenantId, String taskId) {
        LOG
            .info(
                "Sending an HC impute request to process data from timestamp {} to {} for config {}",
                dataStartTime,
                dataEndTime,
                configID
            );

        DiscoveryNode[] dataNodes = hashRing.getNodesWithSameLocalVersion();

        adNodeCommunicator
            .imputeHC(
                new ADHCImputeRequest(configID, tenantId, taskId, dataStartTime, dataEndTime, dataNodes),
                ActionListener.wrap(hcImputeResponse -> {
                    for (final ADHCImputeNodeResponse nodeResponse : hcImputeResponse.getNodes()) {
                        if (nodeResponse.getPreviousException() != null) {
                            nodeStateManager.setException(configID, nodeResponse.getPreviousException());
                        }
                    }
                }, e -> {
                    LOG.warn("fail to HC impute", e);
                    nodeStateManager.setException(configID, e);
                })
            );
    }
}
