/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.client.RestDataPlaneResponse;

public class AbstractSearchActionTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testRestDataPlaneSearchResponseStripsTypedAggregationKeys() throws Exception {
        SearchResponse response = typedSearchResponse(true);

        XContentBuilder builder = AbstractSearchAction.toClientSearchResponse(response, JsonXContent.contentBuilder());
        Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        Map<String, Object> aggregations = (Map<String, Object>) responseMap.get("aggregations");
        Map<String, Object> detectors = (Map<String, Object>) aggregations.get("detectors");
        List<Object> buckets = (List<Object>) detectors.get("buckets");
        Map<String, Object> bucket = (Map<String, Object>) buckets.get(0);
        Map<String, Object> latestTasks = (Map<String, Object>) bucket.get("latest_tasks");
        Map<String, Object> latestTaskHits = (Map<String, Object>) latestTasks.get("hits");
        List<Object> hits = (List<Object>) latestTaskHits.get("hits");
        Map<String, Object> hit = (Map<String, Object>) hits.get(0);
        Map<String, Object> source = (Map<String, Object>) hit.get("_source");

        assertTrue(aggregations.containsKey("detectors"));
        assertFalse(aggregations.containsKey("sterms#detectors"));
        assertTrue(bucket.containsKey("latest_tasks"));
        assertFalse(bucket.containsKey("top_hits#latest_tasks"));
        assertEquals("value", source.get("field#name"));
    }

    @SuppressWarnings("unchecked")
    public void testNativeSearchResponseDoesNotStripTypedAggregationKeys() throws Exception {
        SearchResponse response = typedSearchResponse(false);

        XContentBuilder builder = AbstractSearchAction.toClientSearchResponse(response, JsonXContent.contentBuilder());
        Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        Map<String, Object> aggregations = (Map<String, Object>) responseMap.get("aggregations");
        Map<String, Object> detectors = (Map<String, Object>) aggregations.get("sterms#detectors");
        List<Object> buckets = (List<Object>) detectors.get("buckets");
        Map<String, Object> bucket = (Map<String, Object>) buckets.get(0);

        assertTrue(aggregations.containsKey("sterms#detectors"));
        assertFalse(aggregations.containsKey("detectors"));
        assertTrue(bucket.containsKey("top_hits#latest_tasks"));
        assertFalse(bucket.containsKey("latest_tasks"));
    }

    private SearchResponse typedSearchResponse(boolean restDataPlaneResponse) throws Exception {
        SearchResponse response = restDataPlaneResponse
            ? mock(SearchResponse.class, withSettings().extraInterfaces(RestDataPlaneResponse.class))
            : mock(SearchResponse.class);
        doAnswer(invocation -> {
            XContentBuilder responseBuilder = invocation.getArgument(0);
            responseBuilder.map(typedSearchResponseMap());
            return responseBuilder;
        }).when(response).toXContent(any(XContentBuilder.class), any(ToXContent.Params.class));
        return response;
    }

    private Map<String, Object> typedSearchResponseMap() {
        return Map
            .of(
                "took",
                1,
                "timed_out",
                false,
                "_shards",
                Map.of("total", 1, "successful", 1, "skipped", 0, "failed", 0),
                "hits",
                Map.of("total", Map.of("value", 0, "relation", "eq"), "hits", List.of()),
                "aggregations",
                Map
                    .of(
                        "sterms#detectors",
                        Map
                            .of(
                                "doc_count_error_upper_bound",
                                0,
                                "sum_other_doc_count",
                                0,
                                "buckets",
                                List
                                    .of(
                                        Map
                                            .of(
                                                "key",
                                                "detector-1",
                                                "doc_count",
                                                1,
                                                "top_hits#latest_tasks",
                                                Map
                                                    .of(
                                                        "hits",
                                                        Map
                                                            .of(
                                                                "total",
                                                                Map.of("value", 1, "relation", "eq"),
                                                                "max_score",
                                                                1.0,
                                                                "hits",
                                                                List
                                                                    .of(
                                                                        Map
                                                                            .of(
                                                                                "_index",
                                                                                ".opendistro-anomaly-detection-state",
                                                                                "_id",
                                                                                "task-1",
                                                                                "_score",
                                                                                1.0,
                                                                                "_source",
                                                                                Map.of("field#name", "value")
                                                                            )
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            )
                    )
            );
    }
}
