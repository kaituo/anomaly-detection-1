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

package org.opensearch.timeseries.feature;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.util.ParseUtils;

/**
 *
 * Use pagination to fetch entities.  If there are more than pageSize entities,
 * we will fetch them in the next page. We implement pagination with composite query.
 * Results are decomposed into pages. Each page encapsulates aggregated values for
 * each entity and is sent to model nodes according to the hash ring mapping from
 * entity model Id to a data node.
 *
 */
public class CompositeRetriever extends AbstractRetriever {
    public static final String AGG_NAME_COMP = "comp_agg";
    private static final Logger LOG = LogManager.getLogger(CompositeRetriever.class);

    private final long dataStartEpoch;
    private final long dataEndEpoch;
    private final Config config;
    private final NamedXContentRegistry xContent;
    private final DataAccess dataAccess;
    private int totalResults;
    // we can process at most maxEntities entities
    private int maxEntities;
    private final int pageSize;
    private long expirationEpochMs;
    private Clock clock;
    private AnalysisType context;

    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        Config config,
        NamedXContentRegistry xContent,
        DataAccess dataAccess,
        long expirationEpochMs,
        Clock clock,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize,
        AnalysisType context
    ) {
        this.dataStartEpoch = dataStartEpoch;
        this.dataEndEpoch = dataEndEpoch;
        this.config = config;
        this.xContent = xContent;
        this.dataAccess = dataAccess;
        this.totalResults = 0;
        this.maxEntities = maxEntitiesPerInterval;
        this.pageSize = pageSize;
        this.expirationEpochMs = expirationEpochMs;
        this.clock = clock;
        this.context = context;
    }

    // a constructor that provide default value of clock
    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        Config anomalyDetector,
        NamedXContentRegistry xContent,
        DataAccess dataAccess,
        long expirationEpochMs,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize,
        AnalysisType context
    ) {
        this(
            dataStartEpoch,
            dataEndEpoch,
            anomalyDetector,
            xContent,
            dataAccess,
            expirationEpochMs,
            Clock.systemUTC(),
            settings,
            maxEntitiesPerInterval,
            pageSize,
            context
        );
    }

    /**
     * @return an iterator over pages
     * @throws IOException - if we cannot construct valid queries according to
     *  detector definition
     */
    public PageIterator iterator() throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(config.getTimeField())
            .gte(dataStartEpoch)
            .lt(dataEndEpoch)
            .format("epoch_millis");

        BoolQueryBuilder internalFilterQuery = new BoolQueryBuilder().filter(config.getFilterQuery()).filter(rangeQuery);

        // multiple categorical fields are supported
        CompositeAggregationBuilder composite = AggregationBuilders
            .composite(
                AGG_NAME_COMP,
                config.getCategoryFields().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
            )
            .size(pageSize);
        for (Feature feature : config.getFeatureAttributes()) {
            AggregatorFactories.Builder internalAgg = ParseUtils
                .parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
            composite.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
        }

        // In order to optimize the early termination it is advised to set track_total_hits in the request to false.
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(internalFilterQuery)
            .size(0)
            .aggregation(composite)
            .trackTotalHits(false);

        return new PageIterator(searchSourceBuilder);
    }

    public class PageIterator {
        private SearchSourceBuilder source;
        // a map from categorical field name to values (type: java.lang.Comparable)
        private Map<String, Object> afterKey;
        // number of iterations so far
        private int iterations;
        private long startMs;

        public PageIterator(SearchSourceBuilder source) {
            this.source = source;
            this.afterKey = null;
            this.iterations = 0;
            this.startMs = clock.millis();
        }

        /**
         * Results are returned using listener
         * @param listener Listener to return results
         */
        public void next(ActionListener<Page> listener) {
            iterations++;

            // inject user role while searching.

            SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0]), source);
            final ActionListener<SearchResponse> searchResponseListener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    processResponse(
                        response,
                        () -> dataAccess.search(searchRequest, TenantContext.user(config.getTenantId()), this),
                        listener
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            dataAccess
                .searchWithInjectedSecurity(
                    searchRequest,
                    config.getId(),
                    TenantContext.user(config.getTenantId()),
                    context,
                    searchResponseListener
                );
        }

        private void processResponse(SearchResponse response, Runnable retry, ActionListener<Page> listener) {
            shouldRetryDueToEmptyPage(response, ActionListener.wrap(shouldRetry -> {
                try {
                    if (shouldRetry) {
                        updateCompositeAfterKey(response, source, ActionListener.wrap(v -> retry.run(), listener::onFailure));
                        return;
                    }

                    analyzePage(response, ActionListener.wrap(page -> {
                        if (afterKey != null) {
                            updateCompositeAfterKey(response, source, ActionListener.wrap(v -> listener.onResponse(page), listener::onFailure));
                        } else {
                            listener.onResponse(page);
                        }
                    }, listener::onFailure));
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            }, listener::onFailure));
        }

        /**
         * Analyzes the search response and returns a page containing aggregated results.
         *
         * @param response current response
         * @param listener listener that receives the Page containing:
         *  ** the after key
         *  ** query source builder to next page if any
         *  ** a map of composite keys to its values, arranged according to anomalyDetector.getEnabledFeatureIds()
         */
        private void analyzePage(SearchResponse response, ActionListener<Page> listener) {
            getComposite(response, ActionListener.wrap(compositeOptional -> {
                if (false == compositeOptional.isPresent()) {
                    listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Empty resposne: %s", response)));
                    return;
                }

                CompositeAggregation composite = compositeOptional.get();
                Map<Entity, double[]> results = new HashMap<>();
                /*
                 *
                 * Example composite aggregation:
                 *
                 "aggregations": {
                    "my_buckets": {
                        "after_key": {
                            "service": "app_6",
                            "host": "server_3"
                        },
                        "buckets": [
                            {
                                "key": {
                                    "service": "app_6",
                                    "host": "server_3"
                                },
                                "doc_count": 1,
                                "the_max": {
                                    "value": -38.0
                                },
                                "the_min": {
                                    "value": -38.0
                                }
                            }
                        ]
                   }
                 }
                 */
                for (Bucket bucket : composite.getBuckets()) {
                    Optional<double[]> featureValues = parseBucket(bucket, config.getEnabledFeatureIds(), true);
                    // bucket.getKey() returns a map of categorical field like "host" and its value like "server_1"
                    if (featureValues.isPresent() && bucket.getKey() != null) {
                        results.put(Entity.createEntityByReordering(bucket.getKey()), featureValues.get());
                    }
                }

                totalResults += results.size();

                afterKey = composite.afterKey();
                listener.onResponse(new Page(results));
            }, listener::onFailure));
        }

        private void updateCompositeAfterKey(SearchResponse r, SearchSourceBuilder search, ActionListener<Void> listener) {
            getComposite(r, ActionListener.wrap(composite -> {
                if (false == composite.isPresent()) {
                    listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Empty resposne: %s", r)));
                    return;
                }

                updateSourceAfterKey(composite.get().afterKey(), search);
                listener.onResponse(null);
            }, listener::onFailure));
        }

        private void shouldRetryDueToEmptyPage(SearchResponse response, ActionListener<Boolean> listener) {
            getComposite(response, ActionListener.wrap(composite -> {
                // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
                if (false == composite.isPresent()) {
                    listener.onResponse(false);
                    return;
                }
                CompositeAggregation aggr = composite.get();
                listener.onResponse(aggr.getBuckets().isEmpty() && aggr.afterKey() != null && !aggr.afterKey().isEmpty());
            }, listener::onFailure));
        }

        /**
         * Gets the composite aggregation from the response, resolving index names asynchronously if needed.
         *
         * <p>When the source index is a regex like {@code blah*}, we will get an empty response even if no
         * index starting with "blah" exists:
         * <pre>
         * {"took":0,"timed_out":false,"_shards":{"total":0,"successful":0,"skipped":0,"failed":0},
         *  "hits":{"max_score":0.0,"hits":[]}}
         * </pre>
         *
         * <p>Without regex, we will get an {@link IndexNotFoundException} instead:
         * <pre>
         * {"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [blah]",
         *  "index":"blah","resource.id":"blah","resource.type":"index_or_alias","index_uuid":"_na_"}],
         *  "type":"index_not_found_exception","reason":"no such index [blah]",...},"status":404}
         * </pre>
         *
         * @param response the search response
         * @param listener listener that receives the Optional CompositeAggregation
         */
        void getComposite(SearchResponse response, ActionListener<Optional<CompositeAggregation>> listener) {
            if (response == null || response.getAggregations() == null) {
                List<String> sourceIndices = config.getIndices();
                dataAccess.concreteIndexNames(
                    IndicesOptions.lenientExpandOpen(),
                    sourceIndices.toArray(new String[0]),
                    TenantContext.user(config.getTenantId()),
                    ActionListener.wrap(concreteIndices -> {
                        if (concreteIndices.length == 0) {
                            listener.onFailure(new IndexNotFoundException(String.join(",", sourceIndices)));
                        } else {
                            listener.onResponse(Optional.empty());
                        }
                    }, listener::onFailure)
                );
                return;
            }
            Aggregation agg = response.getAggregations().get(AGG_NAME_COMP);
            if (agg == null) {
                // when current interval has no data
                listener.onResponse(Optional.empty());
                return;
            }

            if (agg instanceof CompositeAggregation) {
                listener.onResponse(Optional.of((CompositeAggregation) agg));
                return;
            }

            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Not a composite response; {}", agg.getClass())));
        }

        /**
         * Whether next page exists.  Conditions are:
         * 1) this is the first time we query (iterations == 0) or afterKey is not null
         * 2) next detection interval has not started
         * @return true if the iteration has more pages.
         */
        public boolean hasNext() {
            long now = clock.millis();
            if (expirationEpochMs <= now) {
                LOG
                    .debug(
                        new ParameterizedMessage(
                            "Time is up, afterKey: [{}], expirationEpochMs: [{}], now [{}]",
                            afterKey,
                            expirationEpochMs,
                            now
                        )
                    );
            }
            LOG
                .debug(
                    new ParameterizedMessage(
                        "Composite retriever state - iterations: [{}], afterKey: [{}], totalResults: [{}], maxEntities: [{}], expirationEpochMs: [{}], now: [{}]",
                        iterations,
                        afterKey,
                        totalResults,
                        maxEntities,
                        expirationEpochMs,
                        now
                    )
                );
            if ((iterations > 0 && afterKey == null) || totalResults > maxEntities) {
                LOG.debug(new ParameterizedMessage("Finished in [{}] msecs. ", (now - startMs)));
            }
            return (iterations == 0 || (totalResults > 0 && afterKey != null)) && expirationEpochMs > now && totalResults <= maxEntities;
        }

        @Override
        public String toString() {
            ToStringBuilder toStringBuilder = new ToStringBuilder(this);

            if (afterKey != null) {
                toStringBuilder.append("afterKey", afterKey);
            }
            if (source != null) {
                toStringBuilder.append("source", source);
            }

            return toStringBuilder.toString();
        }
    }

    public class Page {

        Map<Entity, double[]> results;

        public Page(Map<Entity, double[]> results) {
            this.results = results;
        }

        public boolean isEmpty() {
            return results == null || results.isEmpty();
        }

        public Map<Entity, double[]> getResults() {
            return results;
        }

        @Override
        public String toString() {
            ToStringBuilder toStringBuilder = new ToStringBuilder(this);

            if (results != null) {
                toStringBuilder.append("results", results);
            }

            return toStringBuilder.toString();
        }
    }
}
