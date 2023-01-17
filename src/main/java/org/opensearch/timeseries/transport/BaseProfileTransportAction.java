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

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class BaseProfileTransportAction<RCFModelType extends ThresholdedRandomCutForest, CacheType extends TimeSeriesCache<RCFModelType>, CacheProviderType extends CacheProvider<RCFModelType, CacheType>>
    extends TransportNodesAction<ProfileRequest, ProfileResponse, ProfileNodeRequest, ProfileNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(BaseProfileTransportAction.class);
    private CacheProviderType cacheProvider;
    // the number of models to return. Defaults to 10.
    private volatile int numModelsToReturn;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param cacheProvider cache provider
     * @param settings Node settings accessor
     * @param maxModelNumberPerNode max number of models to show per node
     */
    public BaseProfileTransportAction(
        String profileAction,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CacheProviderType cacheProvider,
        Settings settings,
        Setting<Integer> maxModelNumberPerNode
    ) {
        super(
            profileAction,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ProfileRequest::new,
            ProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ProfileNodeResponse.class
        );
        this.cacheProvider = cacheProvider;
        this.numModelsToReturn = maxModelNumberPerNode.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxModelNumberPerNode, it -> this.numModelsToReturn = it);
    }

    @Override
    protected ProfileResponse newResponse(ProfileRequest request, List<ProfileNodeResponse> responses, List<FailedNodeException> failures) {
        return new ProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ProfileNodeRequest newNodeRequest(ProfileRequest request) {
        return new ProfileNodeRequest(request);
    }

    @Override
    protected ProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ProfileNodeResponse(in);
    }

    @Override
    protected ProfileNodeResponse nodeOperation(ProfileNodeRequest request) {
        String configId = request.getConfigId();
        Set<ProfileName> profiles = request.getProfilesToBeRetrieved();
        int shingleSize = -1;
        long activeEntity = 0;
        long totalUpdates = 0;
        Map<String, Long> modelSize = null;
        List<ModelProfile> modelProfiles = null;
        int modelCount = 0;
        if (profiles.contains(ProfileName.ACTIVE_ENTITIES)) {
            activeEntity = cacheProvider.get().getActiveEntities(configId);
        }

        // state profile requires totalUpdates as well
        if (profiles.contains(ProfileName.INIT_PROGRESS) || profiles.contains(ProfileName.STATE)) {
            totalUpdates = cacheProvider.get().getTotalUpdates(configId);// get toal updates
        }
        if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
            modelSize = cacheProvider.get().getModelSize(configId);
        }
        // need to provide entity info for HCAD
        if (profiles.contains(ProfileName.MODELS)) {
            modelProfiles = cacheProvider.get().getAllModelProfile(configId);
            modelCount = modelProfiles.size();
            int limit = Math.min(numModelsToReturn, modelCount);
            if (limit != modelCount) {
                LOG.info("model number limit reached");
                modelProfiles = modelProfiles.subList(0, limit);
            }
        }

        return new ProfileNodeResponse(
            clusterService.localNode(),
            modelSize,
            shingleSize,
            activeEntity,
            totalUpdates,
            modelProfiles,
            modelCount
        );
    }
}
