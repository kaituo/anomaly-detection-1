/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.client;

import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.client.NodeCommunicator;

/**
 * DI (dependency injection) Marker interface for AD-specific node communication.
 * Wired via TimeSeriesAnalyticsPlugin.
 */
public interface ADNodeCommunicator extends NodeCommunicator {

    /**
     * Broadcast HC impute request to data nodes for imputing missing values in high cardinality detectors.
     *
     * @param request the HC impute request containing configId, tenantId, taskId, and time range
     * @param listener the listener to notify on completion
     */
    void imputeHC(ADHCImputeRequest request, ActionListener<ADHCImputeNodesResponse> listener);
}
