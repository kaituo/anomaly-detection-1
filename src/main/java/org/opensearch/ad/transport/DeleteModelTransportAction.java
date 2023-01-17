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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.ml.ADEntityColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class DeleteModelTransportAction extends
    TransportNodesAction<DeleteModelRequest, DeleteModelResponse, DeleteModelNodeRequest, DeleteModelNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(DeleteModelTransportAction.class);
    private NodeStateManager nodeStateManager;
    private NodeStateManager adNodeStateManager;
    private ADModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider<ThresholdedRandomCutForest, ADPriorityCache> cache;
    private ADTaskCacheManager adTaskCacheManager;
    private ADEntityColdStart coldStarter;

    @Inject
    public DeleteModelTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager nodeStateManager,
        NodeStateManager adNodeStateManager,
        ADModelManager modelManager,
        FeatureManager featureManager,
        CacheProvider<ThresholdedRandomCutForest, ADPriorityCache> cache,
        ADTaskCacheManager adTaskCacheManager,
        ADEntityColdStart coldStarter
    ) {
        super(
            DeleteModelAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            DeleteModelRequest::new,
            DeleteModelNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            DeleteModelNodeResponse.class
        );
        this.nodeStateManager = nodeStateManager;
        this.adNodeStateManager = adNodeStateManager;
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cache = cache;
        this.adTaskCacheManager = adTaskCacheManager;
        this.coldStarter = coldStarter;
    }

    @Override
    protected DeleteModelResponse newResponse(
        DeleteModelRequest request,
        List<DeleteModelNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new DeleteModelResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected DeleteModelNodeRequest newNodeRequest(DeleteModelRequest request) {
        return new DeleteModelNodeRequest(request);
    }

    @Override
    protected DeleteModelNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new DeleteModelNodeResponse(in);
    }

    /**
     *
     * Delete checkpoint document (including both RCF and thresholding model), in-memory models,
     * buffered shingle data, transport state, and anomaly result
     *
     * @param request delete request
     * @return delete response including local node Id.
     */
    @Override
    protected DeleteModelNodeResponse nodeOperation(DeleteModelNodeRequest request) {

        String adID = request.getAdID();
        LOG.info("Delete model for {}", adID);
        // delete in-memory models and model checkpoint
        modelManager
            .clear(
                adID,
                ActionListener
                    .wrap(
                        r -> LOG.info("Deleted model for [{}] with response [{}] ", adID, r),
                        e -> LOG.error("Fail to delete model for " + adID, e)
                    )
            );

        // delete buffered shingle data
        featureManager.clear(adID);

        // delete transport state
        nodeStateManager.clear(adID);
        adNodeStateManager.clear(adID);

        cache.get().clear(adID);

        coldStarter.clear(adID);

        // delete realtime task cache
        adTaskCacheManager.removeRealtimeTaskCache(adID);

        LOG.info("Finished deleting {}", adID);
        return new DeleteModelNodeResponse(clusterService.localNode());
    }

}
