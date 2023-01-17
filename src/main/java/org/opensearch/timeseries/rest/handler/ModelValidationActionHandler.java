/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CONFIG_BUCKET_MINIMUM_SUCCESS_RATE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_TIMES_DECREASING_INTERVAL;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.TOP_VALIDATE_TIMEOUT_IN_MILLIS;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.MergeableList;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;

/**
 * <p>This class executes all validation checks that are not blocking on the 'model' level.
 * This mostly involves checking if the data is generally dense enough to complete model training
 * which is based on if enough buckets in the last x intervals have at least 1 document present.</p>
 * <p>Initially different bucket aggregations are executed with with every configuration applied and with
 * different varying intervals in order to find the best interval for the data. If no interval is found with all
 * configuration applied then each configuration is tested sequentially for sparsity</p>
 */
// TODO: Add more UT and IT
public class ModelValidationActionHandler {
    protected static final String AGG_NAME_TOP = "top_agg";
    protected static final String AGGREGATION = "agg";
    protected final Config config;
    protected final ClusterService clusterService;
    protected final Logger logger = LogManager.getLogger(ModelValidationActionHandler.class);
    protected final TimeValue requestTimeout;
    protected final ConfigUpdateConfirmer handler = new ConfigUpdateConfirmer();
    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ActionListener<ValidateConfigResponse> listener;
    protected final SearchFeatureDao searchFeatureDao;
    protected final Clock clock;
    protected final String validationType;
    protected final Settings settings;
    protected final User user;
    protected final AnalysisType context;

    /**
     * Constructor function.
     *
     * @param clusterService                  ClusterService
     * @param client                          OS node client that executes actions on the local node
     * @param clientUtil                      client util
     * @param listener                        OS channel used to construct bytes / builder based outputs, and send responses
     * @param config                          config instance
     * @param requestTimeout                  request time out configuration
     * @param xContentRegistry                Registry which is used for XContentParser
     * @param searchFeatureDao                Search feature DAO
     * @param validationType                  Specified type for validation
     * @param clock                           clock object to know when to timeout
     * @param settings                        Node settings
     * @param user                            User info
     */
    public ModelValidationActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ActionListener<ValidateConfigResponse> listener,
        Config config,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock,
        Settings settings,
        User user,
        AnalysisType context
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.clientUtil = clientUtil;
        this.listener = listener;
        this.config = config;
        this.requestTimeout = requestTimeout;
        this.xContentRegistry = xContentRegistry;
        this.searchFeatureDao = searchFeatureDao;
        this.validationType = validationType;
        this.clock = clock;
        this.settings = settings;
        this.user = user;
        this.context = context;
    }

    // Need to first check if HC analysis or not before doing any sort of validation.
    // If detector is HC then we will find the top entity and treat as single stream for
    // validation purposes
    public void checkIfHC() {
        ActionListener<Map<String, Object>> recommendationListener = ActionListener
            .wrap(topEntity -> getLatestDateForValidation(topEntity), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get top entity for categorical field", exception);
            });
        if (config.isHighCardinality()) {
            getTopEntity(recommendationListener);
        } else {
            recommendationListener.onResponse(Collections.emptyMap());
        }
    }

    // For single category HCs, this method uses bucket aggregation and sort to get the category field
    // that have the highest document count in order to use that top entity for further validation
    // For multi-category HCs we use a composite aggregation to find the top fields for the entity
    // with the highest doc count.
    private void getTopEntity(ActionListener<Map<String, Object>> topEntityListener) {
        // Look at data back to the lower bound given the max interval we recommend or one given
        long maxIntervalInMinutes = Math.max(MAX_INTERVAL_REC_LENGTH_IN_MINUTES, config.getIntervalInMinutes());
        LongBounds timeRangeBounds = getTimeRangeBounds(
            Instant.now().toEpochMilli(),
            new IntervalTimeConfiguration(maxIntervalInMinutes, ChronoUnit.MINUTES)
        );
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(config.getTimeField())
            .from(timeRangeBounds.getMin())
            .to(timeRangeBounds.getMax());
        AggregationBuilder bucketAggs;
        Map<String, Object> topKeys = new HashMap<>();
        if (config.getCategoryFields().size() == 1) {
            bucketAggs = AggregationBuilders.terms(AGG_NAME_TOP).field(config.getCategoryFields().get(0)).order(BucketOrder.count(true));
        } else {
            bucketAggs = AggregationBuilders
                .composite(
                    AGG_NAME_TOP,
                    config.getCategoryFields().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
                )
                .size(1000)
                .subAggregation(
                    PipelineAggregatorBuilders
                        .bucketSort("bucketSort", Collections.singletonList(new FieldSortBuilder("_count").order(SortOrder.DESC)))
                        .size(1)
                );
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(rangeQuery)
            .aggregation(bucketAggs)
            .trackTotalHits(false)
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                topEntityListener.onResponse(Collections.emptyMap());
                return;
            }
            if (config.getCategoryFields().size() == 1) {
                Terms entities = aggs.get(AGG_NAME_TOP);
                Object key = entities
                    .getBuckets()
                    .stream()
                    .max(Comparator.comparingInt(entry -> (int) entry.getDocCount()))
                    .map(MultiBucketsAggregation.Bucket::getKeyAsString)
                    .orElse(null);
                topKeys.put(config.getCategoryFields().get(0), key);
            } else {
                CompositeAggregation compositeAgg = aggs.get(AGG_NAME_TOP);
                topKeys
                    .putAll(
                        compositeAgg
                            .getBuckets()
                            .stream()
                            .flatMap(bucket -> bucket.getKey().entrySet().stream()) // this would create a flattened stream of map entries
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))
                    );
            }
            for (Map.Entry<String, Object> entry : topKeys.entrySet()) {
                if (entry.getValue() == null) {
                    topEntityListener.onResponse(Collections.emptyMap());
                    return;
                }
            }
            topEntityListener.onResponse(topKeys);
        }, topEntityListener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private void getLatestDateForValidation(Map<String, Object> topEntity) {
        ActionListener<Optional<Long>> latestTimeListener = ActionListener
            .wrap(latest -> getSampleRangesForValidationChecks(latest, config, listener, topEntity), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to create search request for last data point", exception);
            });
        searchFeatureDao.getLatestDataTime(config, context, latestTimeListener);
    }

    private void getSampleRangesForValidationChecks(
        Optional<Long> latestTime,
        Config config,
        ActionListener<ValidateConfigResponse> listener,
        Map<String, Object> topEntity
    ) {
        if (!latestTime.isPresent() || latestTime.get() <= 0) {
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.TIME_FIELD_NOT_ENOUGH_HISTORICAL_DATA,
                        ValidationIssueType.TIMEFIELD_FIELD,
                        ValidationAspect.MODEL
                    )
                );
            return;
        }
        long timeRangeEnd = Math.min(Instant.now().toEpochMilli(), latestTime.get());
        try {
            getBucketAggregates(timeRangeEnd, listener, topEntity);
        } catch (IOException e) {
            listener.onFailure(new EndRunException(config.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, true));
        }
    }

    private void getBucketAggregates(long latestTime, ActionListener<ValidateConfigResponse> listener, Map<String, Object> topEntity)
        throws IOException {
        AggregationBuilder aggregation = getBucketAggregation(latestTime, (IntervalTimeConfiguration) config.getInterval());
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        if (config.isHighCardinality()) {
            if (topEntity.isEmpty()) {
                listener
                    .onFailure(
                        new ValidationException(
                            CommonMessages.CATEGORY_FIELD_TOO_SPARSE,
                            ValidationIssueType.CATEGORY,
                            ValidationAspect.MODEL
                        )
                    );
                return;
            }
            for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
                query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(query)
            .aggregation(aggregation)
            .size(0)
            .timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        ActionListener<IntervalTimeConfiguration> intervalListener = ActionListener
            .wrap(interval -> processIntervalRecommendation(interval, latestTime), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get interval recommendation", exception);
            });
        final ActionListener<SearchResponse> searchResponseListener = new ModelValidationActionHandler.IntervalRecommendationListener(
            intervalListener,
            searchRequest.source(),
            (IntervalTimeConfiguration) config.getInterval(),
            clock.millis() + TOP_VALIDATE_TIMEOUT_IN_MILLIS,
            latestTime,
            false,
            MAX_TIMES_DECREASING_INTERVAL
        );
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private double processBucketAggregationResults(Histogram buckets) {
        int docCountOverOne = 0;
        // For each entry
        for (Histogram.Bucket entry : buckets.getBuckets()) {
            if (entry.getDocCount() > 0) {
                docCountOverOne++;
            }
        }
        return (docCountOverOne / (double) getNumberOfSamples());
    }

    /**
     * ActionListener class to handle execution of multiple bucket aggregations one after the other
     * Bucket aggregation with different interval lengths are executed one by one to check if the data is dense enough
     * We only need to execute the next query if the previous one led to data that is too sparse.
     */
    class IntervalRecommendationListener implements ActionListener<SearchResponse> {
        private final ActionListener<IntervalTimeConfiguration> intervalListener;
        SearchSourceBuilder searchSourceBuilder;
        IntervalTimeConfiguration configInterval;
        private final long expirationEpochMs;
        private final long latestTime;
        boolean decreasingInterval;
        int numTimesDecreasing; // maximum amount of times we will try decreasing interval for recommendation

        IntervalRecommendationListener(
            ActionListener<IntervalTimeConfiguration> intervalListener,
            SearchSourceBuilder searchSourceBuilder,
            IntervalTimeConfiguration configInterval,
            long expirationEpochMs,
            long latestTime,
            boolean decreasingInterval,
            int numTimesDecreasing
        ) {
            this.intervalListener = intervalListener;
            this.searchSourceBuilder = searchSourceBuilder;
            this.configInterval = configInterval;
            this.expirationEpochMs = expirationEpochMs;
            this.latestTime = latestTime;
            this.decreasingInterval = decreasingInterval;
            this.numTimesDecreasing = numTimesDecreasing;
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                Histogram aggregate = checkBucketResultErrors(response);
                if (aggregate == null) {
                    return;
                }

                long newIntervalMinute;
                // configInterval is changed in every iteration inside searchWithDifferentInterval
                if (decreasingInterval) {
                    newIntervalMinute = (long) Math
                        .floor(
                            IntervalTimeConfiguration.getIntervalInMinute(configInterval) * INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER
                        );
                } else {
                    newIntervalMinute = (long) Math
                        .ceil(
                            IntervalTimeConfiguration.getIntervalInMinute(configInterval) * INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER
                        );
                }
                double fullBucketRate = processBucketAggregationResults(aggregate);
                // If rate is above success minimum then return interval suggestion.
                if (fullBucketRate > INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE) {
                    intervalListener.onResponse(this.configInterval);
                } else if (expirationEpochMs < clock.millis()) {
                    listener
                        .onFailure(
                            new ValidationException(
                                CommonMessages.TIMEOUT_ON_INTERVAL_REC,
                                ValidationIssueType.TIMEOUT,
                                ValidationAspect.MODEL
                            )
                        );
                    logger.info(CommonMessages.TIMEOUT_ON_INTERVAL_REC);
                    // keep trying higher intervals as new interval is below max, and we aren't decreasing yet
                } else if (newIntervalMinute < MAX_INTERVAL_REC_LENGTH_IN_MINUTES && !decreasingInterval) {
                    searchWithDifferentInterval(newIntervalMinute);
                    // The below block is executed only the first time when new interval is above max and
                    // we aren't decreasing yet, at this point we will start decreasing for the first time
                    // if we are inside the below block
                } else if (newIntervalMinute >= MAX_INTERVAL_REC_LENGTH_IN_MINUTES && !decreasingInterval) {
                    IntervalTimeConfiguration givenInterval = (IntervalTimeConfiguration) config.getInterval();
                    this.configInterval = new IntervalTimeConfiguration(
                        (long) Math
                            .floor(
                                IntervalTimeConfiguration.getIntervalInMinute(givenInterval) * INTERVAL_RECOMMENDATION_DECREASING_MULTIPLIER
                            ),
                        ChronoUnit.MINUTES
                    );
                    if (configInterval.getInterval() <= 0) {
                        intervalListener.onResponse(null);
                        return;
                    }
                    this.decreasingInterval = true;
                    this.numTimesDecreasing -= 1;
                    // Searching again using an updated interval
                    SearchSourceBuilder updatedSearchSourceBuilder = getSearchSourceBuilder(
                        searchSourceBuilder.query(),
                        getBucketAggregation(this.latestTime, new IntervalTimeConfiguration(newIntervalMinute, ChronoUnit.MINUTES))
                    );
                    // using the original context in listener as user roles have no permissions for internal operations like fetching a
                    // checkpoint
                    clientUtil
                        .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                            new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(updatedSearchSourceBuilder),
                            client::search,
                            user,
                            client,
                            context,
                            this
                        );
                    // In this case decreasingInterval has to be true already, so we will stop
                    // when the next new interval is below or equal to 0, or we have decreased up to max times
                } else if (numTimesDecreasing >= 0 && newIntervalMinute > 0) {
                    this.numTimesDecreasing -= 1;
                    searchWithDifferentInterval(newIntervalMinute);
                    // this case means all intervals up to max interval recommendation length and down to either
                    // 0 or until we tried 10 lower intervals than the one given have been tried
                    // which further means the next step is to go through A/B validation checks
                } else {
                    intervalListener.onResponse(null);
                }

            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void searchWithDifferentInterval(long newIntervalMinuteValue) {
            this.configInterval = new IntervalTimeConfiguration(newIntervalMinuteValue, ChronoUnit.MINUTES);
            // Searching again using an updated interval
            SearchSourceBuilder updatedSearchSourceBuilder = getSearchSourceBuilder(
                searchSourceBuilder.query(),
                getBucketAggregation(this.latestTime, new IntervalTimeConfiguration(newIntervalMinuteValue, ChronoUnit.MINUTES))
            );
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(updatedSearchSourceBuilder),
                    client::search,
                    user,
                    client,
                    context,
                    this
                );
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to recommend new interval", e);
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        ValidationIssueType.AGGREGATION,
                        ValidationAspect.MODEL
                    )
                );
        }
    }

    private void processIntervalRecommendation(IntervalTimeConfiguration interval, long latestTime) {
        // if interval suggestion is null that means no interval could be found with all the configurations
        // applied, our next step then is to check density just with the raw data and then add each configuration
        // one at a time to try and find root cause of low density
        if (interval == null) {
            checkRawDataSparsity(latestTime);
        } else {
            if (interval.equals(config.getInterval())) {
                logger.info("Using the current interval there is enough dense data ");
                // Check if there is a window delay recommendation if everything else is successful and send exception
                if (Instant.now().toEpochMilli() - latestTime > timeConfigToMilliSec(config.getWindowDelay())) {
                    sendWindowDelayRec(latestTime);
                    return;
                }
                // The rate of buckets with at least 1 doc with given interval is above the success rate
                listener.onResponse(null);
                return;
            }
            // return response with interval recommendation
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.INTERVAL_REC + interval.getInterval(),
                        ValidationIssueType.DETECTION_INTERVAL,
                        ValidationAspect.MODEL,
                        interval
                    )
                );
        }
    }

    private AggregationBuilder getBucketAggregation(long latestTime, IntervalTimeConfiguration detectorInterval) {
        return AggregationBuilders
            .dateHistogram(AGGREGATION)
            .field(config.getTimeField())
            .minDocCount(1)
            .hardBounds(getTimeRangeBounds(latestTime, detectorInterval))
            .fixedInterval(DateHistogramInterval.minutes((int) IntervalTimeConfiguration.getIntervalInMinute(detectorInterval)));
    }

    private SearchSourceBuilder getSearchSourceBuilder(QueryBuilder query, AggregationBuilder aggregation) {
        return new SearchSourceBuilder().query(query).aggregation(aggregation).size(0).timeout(requestTimeout);
    }

    private void checkRawDataSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime, (IntervalTimeConfiguration) config.getInterval());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(aggregation).size(0).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processRawDataResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private Histogram checkBucketResultErrors(SearchResponse response) {
        Aggregations aggs = response.getAggregations();
        if (aggs == null) {
            // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
            // the large amounts of changes there). For this reason I'm not throwing a SearchException but instead a validation exception
            // which will be converted to validation response.
            logger.warn("Unexpected null aggregation.");
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        ValidationIssueType.AGGREGATION,
                        ValidationAspect.MODEL
                    )
                );
            return null;
        }
        Histogram aggregate = aggs.get(AGGREGATION);
        if (aggregate == null) {
            listener.onFailure(new IllegalArgumentException("Failed to find valid aggregation result"));
            return null;
        }
        return aggregate;
    }

    private void processRawDataResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(CommonMessages.RAW_DATA_TOO_SPARSE, ValidationIssueType.INDICES, ValidationAspect.MODEL)
                );
        } else {
            checkDataFilterSparsity(latestTime);
        }
    }

    private void checkDataFilterSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime, (IntervalTimeConfiguration) config.getInterval());
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processDataFilterResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private void processDataFilterResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.FILTER_QUERY_TOO_SPARSE,
                        ValidationIssueType.FILTER_QUERY,
                        ValidationAspect.MODEL
                    )
                );
            // blocks below are executed if data is dense enough with filter query applied.
            // If HCAD then category fields will be added to bucket aggregation to see if they
            // are the root cause of the issues and if not the feature queries will be checked for sparsity
        } else if (config.isHighCardinality()) {
            getTopEntityForCategoryField(latestTime);
        } else {
            try {
                checkFeatureQueryDelegate(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void getTopEntityForCategoryField(long latestTime) {
        ActionListener<Map<String, Object>> getTopEntityListener = ActionListener
            .wrap(topEntity -> checkCategoryFieldSparsity(topEntity, latestTime), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get top entity for categorical field", exception);
                return;
            });
        getTopEntity(getTopEntityListener);
    }

    private void checkCategoryFieldSparsity(Map<String, Object> topEntity, long latestTime) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
            query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
        }
        AggregationBuilder aggregation = getBucketAggregation(latestTime, (IntervalTimeConfiguration) config.getInterval());
        SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processTopEntityResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private void processTopEntityResults(SearchResponse response, long latestTime) {
        Histogram aggregate = checkBucketResultErrors(response);
        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(CommonMessages.CATEGORY_FIELD_TOO_SPARSE, ValidationIssueType.CATEGORY, ValidationAspect.MODEL)
                );
        } else {
            try {
                checkFeatureQueryDelegate(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void checkFeatureQueryDelegate(long latestTime) throws IOException {
        ActionListener<MergeableList<double[]>> validateFeatureQueriesListener = ActionListener.wrap(response -> {
            windowDelayRecommendation(latestTime);
        }, exception -> {
            listener
                .onFailure(new ValidationException(exception.getMessage(), ValidationIssueType.FEATURE_ATTRIBUTES, ValidationAspect.MODEL));
        });
        MultiResponsesDelegateActionListener<MergeableList<double[]>> multiFeatureQueriesResponseListener =
            new MultiResponsesDelegateActionListener<>(
                validateFeatureQueriesListener,
                config.getFeatureAttributes().size(),
                CommonMessages.FEATURE_QUERY_TOO_SPARSE,
                false
            );

        for (Feature feature : config.getFeatureAttributes()) {
            AggregationBuilder aggregation = getBucketAggregation(latestTime, (IntervalTimeConfiguration) config.getInterval());
            BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
            List<String> featureFields = ParseUtils.getFieldNamesForFeature(feature, xContentRegistry);
            for (String featureField : featureFields) {
                query.filter(QueryBuilders.existsQuery(featureField));
            }
            SearchSourceBuilder searchSourceBuilder = getSearchSourceBuilder(query, aggregation);
            SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
            final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
                Histogram aggregate = checkBucketResultErrors(response);
                if (aggregate == null) {
                    return;
                }
                double fullBucketRate = processBucketAggregationResults(aggregate);
                if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
                    multiFeatureQueriesResponseListener
                        .onFailure(
                            new ValidationException(
                                CommonMessages.FEATURE_QUERY_TOO_SPARSE,
                                ValidationIssueType.FEATURE_ATTRIBUTES,
                                ValidationAspect.MODEL
                            )
                        );
                } else {
                    multiFeatureQueriesResponseListener
                        .onResponse(new MergeableList<>(new ArrayList<>(Collections.singletonList(new double[] { fullBucketRate }))));
                }
            }, e -> {
                logger.error(e);
                multiFeatureQueriesResponseListener
                    .onFailure(new OpenSearchStatusException(CommonMessages.FEATURE_QUERY_TOO_SPARSE, RestStatus.BAD_REQUEST, e));
            });
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    searchRequest,
                    client::search,
                    user,
                    client,
                    context,
                    searchResponseListener
                );
        }
    }

    private void sendWindowDelayRec(long latestTimeInMillis) {
        long minutesSinceLastStamp = (long) Math.ceil((Instant.now().toEpochMilli() - latestTimeInMillis) / 60000.0);
        listener
            .onFailure(
                new ValidationException(
                    String.format(Locale.ROOT, CommonMessages.WINDOW_DELAY_REC, minutesSinceLastStamp, minutesSinceLastStamp),
                    ValidationIssueType.WINDOW_DELAY,
                    ValidationAspect.MODEL,
                    new IntervalTimeConfiguration(minutesSinceLastStamp, ChronoUnit.MINUTES)
                )
            );
    }

    private void windowDelayRecommendation(long latestTime) {
        // Check if there is a better window-delay to recommend and if one was recommended
        // then send exception and return, otherwise continue to let user know data is too sparse as explained below
        if (Instant.now().toEpochMilli() - latestTime > timeConfigToMilliSec(config.getWindowDelay())) {
            sendWindowDelayRec(latestTime);
            return;
        }
        // This case has been reached if following conditions are met:
        // 1. no interval recommendation was found that leads to a bucket success rate of >= 0.75
        // 2. bucket success rate with the given interval and just raw data is also below 0.75.
        // 3. no single configuration during the following checks reduced the bucket success rate below 0.25
        // This means the rate with all configs applied or just raw data was below 0.75 but the rate when checking each configuration at
        // a time was always above 0.25 meaning the best suggestion is to simply ingest more data or change interval since
        // we have no more insight regarding the root cause of the lower density.
        listener
            .onFailure(new ValidationException(CommonMessages.RAW_DATA_TOO_SPARSE, ValidationIssueType.INDICES, ValidationAspect.MODEL));
    }

    private LongBounds getTimeRangeBounds(long endMillis, IntervalTimeConfiguration detectorIntervalInMinutes) {
        Long detectorInterval = timeConfigToMilliSec(detectorIntervalInMinutes);
        Long startMillis = endMillis - (getNumberOfSamples() * detectorInterval);
        return new LongBounds(startMillis, endMillis);
    }

    private int getNumberOfSamples() {
        long interval = config.getIntervalInMilliseconds();
        return Math
            .max(
                (int) (Duration.ofHours(TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS).toMillis() / interval),
                TimeSeriesSettings.MIN_TRAIN_SAMPLES
            );
    }

    private Long timeConfigToMilliSec(TimeConfiguration config) {
        return Optional.ofNullable((IntervalTimeConfiguration) config).map(t -> t.toDuration().toMillis()).orElse(0L);
    }
}
