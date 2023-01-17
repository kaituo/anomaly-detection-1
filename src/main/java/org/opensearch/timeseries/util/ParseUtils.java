package org.opensearch.timeseries.util;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ADParseUtils;
import org.opensearch.common.ParsingException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.BaseAggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.FeatureData;

import com.carrotsearch.hppc.DoubleArrayList;

public class ParseUtils {

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant toInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() == null || parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken().isValue()) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }

    /**
     * Check if two lists of string equals or not without considering the order.
     * If the list is null, will consider it equals to empty list.
     *
     * @param list1 first list
     * @param list2 second list
     * @return true if two list of string equals
     */
    public static boolean listEqualsWithoutConsideringOrder(List<String> list1, List<String> list2) {
        Set<String> set1 = new HashSet<>();
        Set<String> set2 = new HashSet<>();
        if (list1 != null) {
            set1.addAll(list1);
        }
        if (list2 != null) {
            set2.addAll(list2);
        }
        return Objects.equals(set1, set2);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param level   aggregation level, the top level start from 0
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, int level, String aggName) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation."
                );
            }
            final String aggregationName = aggName == null ? parser.currentName() : aggName;
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Invalid aggregation name ["
                        + aggregationName
                        + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'"
                );
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Aggregation definition for ["
                        + aggregationName
                        + " starts with a ["
                        + token
                        + "], expected a ["
                        + XContentParser.Token.START_OBJECT
                        + "]."
                );
            }

            BaseAggregationBuilder aggBuilder = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.FIELD_NAME
                            + "] under a ["
                            + XContentParser.Token.START_OBJECT
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]",
                        parser.getTokenLocation()
                    );
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                        case "meta":
                            metaData = parser.map();
                            break;
                        case "aggregations":
                        case "aggs":
                            if (subFactories != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two sub aggregation definitions under [" + aggregationName + "]"
                                );
                            }
                            subFactories = parseAggregators(parser, level + 1, null);
                            break;
                        default:
                            if (aggBuilder != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two aggregation type definitions in ["
                                        + aggregationName
                                        + "]: ["
                                        + aggBuilder.getType()
                                        + "] and ["
                                        + fieldName
                                        + "]"
                                );
                            }

                            aggBuilder = parser.namedObject(BaseAggregationBuilder.class, fieldName, aggregationName);
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.START_OBJECT
                            + "] under ["
                            + fieldName
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]"
                    );
                }
            }

            if (aggBuilder == null) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Missing definition for aggregation [" + aggregationName + "]",
                    parser.getTokenLocation()
                );
            } else {
                if (metaData != null) {
                    aggBuilder.setMetadata(metaData);
                }

                if (subFactories != null) {
                    aggBuilder.subAggregations(subFactories);
                }

                if (aggBuilder instanceof AggregationBuilder) {
                    factories.addAggregator((AggregationBuilder) aggBuilder);
                } else {
                    factories.addPipelineAggregator((PipelineAggregationBuilder) aggBuilder);
                }
            }
        }

        return factories;
    }

    /**
     * parse aggregation String into {@link AggregatorFactories.Builder}.
     *
     * @param aggQuery         aggregation query string
     * @param xContentRegistry ES named content registry
     * @param aggName          aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(String aggQuery, NamedXContentRegistry xContentRegistry, String aggName)
        throws IOException {
        XContentParser parser = ADParseUtils.parser(aggQuery, xContentRegistry);
        return ParseUtils.parseAggregators(parser, aggName);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, String aggName) throws IOException {
        return parseAggregators(parser, 0, aggName);
    }

    public static <S, T> boolean isNullOrEmpty(Map<S, T> map) {
        return map == null || map.size() == 0;
    }

    public static <T> boolean isNullOrEmpty(Collection<T> collection) {
        return collection == null || collection.size() == 0;
    }

    public static double[] parseDoubleArray(XContentParser parser) throws IOException {
        DoubleArrayList oldValList = new DoubleArrayList();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            oldValList.add(parser.doubleValue());
        }
        return oldValList.toArray();
    }

    /**
     * Map feature data to its Id and name
     * @param currentFeature Feature data
     * @param config Analysis Config object
     * @return a list of feature data with Id and name
     */
    public static List<FeatureData> getFeatureData(double[] currentFeature, Config config) {
        List<String> featureIds = config.getEnabledFeatureIds();
        List<String> featureNames = config.getEnabledFeatureNames();
        int featureLen = featureIds.size();
        List<FeatureData> featureData = new ArrayList<>();
        for (int i = 0; i < featureLen; i++) {
            featureData.add(new FeatureData(featureIds.get(i), featureNames.get(i), currentFeature[i]));
        }
        return featureData;
    }

    public static SearchSourceBuilder generateColdStartQuery(
        Config detector,
        List<Entry<Long, Long>> ranges,
        Optional<Entity> entity,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(detector.getFilterQuery());

        if (entity.isPresent()) {
            for (TermQueryBuilder term : entity.get().getTermQueryBuilders()) {
                internalFilterQuery.filter(term);
            }
        }

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(detector.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(internalFilterQuery).size(0).aggregation(dateRangeBuilder);
    }

}
