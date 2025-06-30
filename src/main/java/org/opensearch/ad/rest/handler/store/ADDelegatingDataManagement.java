/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler.store;

import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.timeseries.rest.handler.store.DataManagement;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;

/**
 * Delegating config store specialized for anomaly detector resources.
 */
public class ADDelegatingDataManagement extends DelegatingDataManagement<ADIndex> {

    public ADDelegatingDataManagement(DataManagement<ADIndex> indexStore, DataManagement<ADIndex> sdkStore, ClusterService clusterService) {
        super(indexStore, sdkStore, clusterService, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);
    }
}
