/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;

import java.util.Collection;
import java.util.Collections;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;

/**
 * Guards the hard-coded aggregation contract for searches executed against the AD config index.
 * <p>
 * Transport-backed config stores can pass arbitrary OpenSearch aggregations through to the cluster.
 * Some non-transport ConfigDocumentStore implementations, such as remote metadata or third-party
 * stores, have to translate each supported aggregation into a store-specific request shape instead.
 * Those implementations typically cannot execute arbitrary aggregation builders on arbitrary fields.
 * <p>
 * If production code adds another config-index aggregation, this test should fail and force the
 * change to either extend the store-specific allow-list/parser or consciously reject the new request
 * shape with a clear error.
 */
public class ConfigIndexAggregationContractTests extends OpenSearchTestCase {

    private static final String TENANT_ID = "account-id:application-id:workspace-id";

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testStatsDetectorCountUsesOnlyDetectorTypeTermsAggregation() {
        DataAccess dataAccess = mock(DataAccess.class);
        ADDelegatingDataManagement adDataManagement = mock(ADDelegatingDataManagement.class);
        when(adDataManagement.doesConfigIndexExist()).thenReturn(true);

        StatsAnomalyDetectorTransportAction action = new StatsAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            new ADStats(Collections.emptyMap()),
            mock(ClusterService.class),
            adDataManagement,
            dataAccess,
            mock(RunContext.class),
            mock(ADNodeCommunicator.class)
        );

        StatsRequest request = new StatsRequest(TENANT_ID, "node-1");
        request.addStat(StatNames.DETECTOR_COUNT.getName());
        action.getClusterStats(mock(MultiResponsesDelegateActionListener.class), request);

        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(dataAccess).search(requestCaptor.capture(), any(TenantContext.class), any(ActionListener.class));

        SearchRequest searchRequest = requestCaptor.getValue();
        assertArrayEquals(new String[] { ADCommonName.CONFIG_INDEX }, searchRequest.indices());
        assertEquals(0, searchRequest.source().size());

        TermsAggregationBuilder aggregation = assertSingleTermsAggregation(
            searchRequest,
            StatsAnomalyDetectorTransportAction.DETECTOR_TYPE_AGG
        );
        assertEquals(AnomalyDetector.DETECTOR_TYPE_FIELD, aggregation.field());
    }

    @Test
    public void testAnomalyResultSearchUsesOnlyResultIndexTermsAggregationOnConfigIndex() {
        SearchAnomalyResultTransportAction action = new SearchAnomalyResultTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ADSearchHandler.class),
            mock(ClusterService.class),
            mock(IndexNameExpressionResolver.class),
            mock(DataAccess.class),
            mock(RunContext.class)
        );

        SearchRequest searchRequest = action.createSingleSearchRequest();

        assertArrayEquals(new String[] { ADCommonName.CONFIG_INDEX }, searchRequest.indices());
        assertEquals(0, searchRequest.source().size());

        TermsAggregationBuilder aggregation = assertSingleTermsAggregation(
            searchRequest,
            SearchAnomalyResultTransportAction.RESULT_INDEX_AGG_NAME
        );
        assertEquals(AnomalyDetector.RESULT_INDEX_FIELD, aggregation.field());
        assertEquals(MAX_DETECTOR_UPPER_LIMIT, aggregation.size());
    }

    private TermsAggregationBuilder assertSingleTermsAggregation(SearchRequest request, String aggregationName) {
        assertNotNull(request.source());
        AggregatorFactories.Builder aggregations = request.source().aggregations();
        assertNotNull(aggregations);
        assertEquals(1, aggregations.count());

        Collection<AggregationBuilder> aggregationBuilders = aggregations.getAggregatorFactories();
        assertEquals(1, aggregationBuilders.size());
        AggregationBuilder aggregation = aggregationBuilders.iterator().next();

        assertTrue(aggregation instanceof TermsAggregationBuilder);
        assertEquals(aggregationName, aggregation.getName());
        assertTrue(aggregation.getSubAggregations().isEmpty());
        assertTrue(aggregation.getPipelineAggregations().isEmpty());

        return (TermsAggregationBuilder) aggregation;
    }
}
