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

package org.opensearch.ad.rest.handler;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.transport.TransportService;

/**
 * Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for creating anomaly detector.
 * PUT request is for updating anomaly detector.
 */
public class IndexAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<IndexAnomalyDetectorResponse> {

    public IndexAnomalyDetectorActionHandler(
        ClusterService clusterService,
        DataAccess dataAccess,
        TransportService transportService,
        ADDelegatingDataManagement anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleStreamDetectors,
        Integer maxHCDetectors,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        Settings settings,
        RunContext runContext
    ) {
        super(
            clusterService,
            dataAccess,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            anomalyDetector,
            requestTimeout,
            maxSingleStreamDetectors,
            maxHCDetectors,
            maxFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry,
            user,
            adTaskManager,
            searchFeatureDao,
            null,
            false,
            null,
            settings,
            runContext
        );
    }
}
