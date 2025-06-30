/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Collections;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.internal.InternalSearchResponse;

final class SdkSearchResponseUtils {

    private SdkSearchResponseUtils() {}

    static boolean isEmptySdkSearchParseFailure(Throwable t) {
        return t instanceof AssertionError && t.getMessage() != null && t.getMessage().contains("total: 0");
    }

    static SearchResponse emptySearchResponse() {
        return new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            0,
            0,
            0,
            0,
            new SearchResponse.PhaseTook(Collections.emptyMap()),
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY,
            null
        );
    }
}
