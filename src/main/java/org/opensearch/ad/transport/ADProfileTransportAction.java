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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.BaseProfileTransportAction;
import org.opensearch.timeseries.transport.ProfileNodeRequest;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ADProfileTransportAction extends BaseProfileTransportAction<ThresholdedRandomCutForest, ADPriorityCache, ADCacheProvider> {
    private ADModelManager modelManager;
    private FeatureManager featureManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param modelManager model manager object
     * @param featureManager feature manager object
     * @param cacheProvider cache provider
     * @param settings Node settings accessor
     */
    @Inject
    public ADProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADModelManager modelManager,
        FeatureManager featureManager,
        ADCacheProvider cacheProvider,
        Settings settings
    ) {
        super(
            ADProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            cacheProvider,
            settings,
            AD_MAX_MODEL_SIZE_PER_NODE
        );
        this.modelManager = modelManager;
        this.featureManager = featureManager;
    }

    @Override
    protected ProfileNodeResponse nodeOperation(ProfileNodeRequest request) {
        String detectorId = request.getConfigId();
        Set<ProfileName> profiles = request.getProfilesToBeRetrieved();
        int shingleSize = -1;
        long activeEntity = 0;
        long totalUpdates = 0;
        Map<String, Long> modelSize = null;
        List<ModelProfile> modelProfiles = null;
        int modelCount = 0;
        if (request.isModelInPriorityCache()) {
            super.nodeOperation(request);
        } else {
            if (profiles.contains(ProfileName.COORDINATING_NODE) || profiles.contains(ProfileName.SHINGLE_SIZE)) {
                shingleSize = featureManager.getShingleSize(detectorId);
            }

            if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES) || profiles.contains(ProfileName.MODELS)) {
                modelSize = modelManager.getModelSize(detectorId);
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
