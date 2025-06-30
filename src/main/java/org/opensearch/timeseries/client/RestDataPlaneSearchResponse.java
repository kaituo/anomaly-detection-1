/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.action.search.SearchResponse;

/**
 * Search response reconstructed from a data-plane REST response.
 */
public final class RestDataPlaneSearchResponse extends SearchResponse implements RestDataPlaneResponse {

    public static RestDataPlaneSearchResponse from(SearchResponse response) {
        return new RestDataPlaneSearchResponse(response);
    }

    private RestDataPlaneSearchResponse(SearchResponse response) {
        super(
            Objects.requireNonNull(response, "response must not be null").getInternalResponse(),
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getPhaseTook(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }
}
