/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.FilterBy;
import org.opensearch.forecast.model.Subaggregation;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.util.ParseUtils;

/**
 * Request for getting the top forecast results for HC forecasters.
 * <p>
 * forecasterId, filterBy, and forecastFrom are required.
 * One or two of buildInQuery, entity, threshold, filterQuery, subaggregations will be set to
 * appropriate value depending on filterBy.
 * Other parameters will be set to default values if left blank.
 */
public class SearchTopForecastResultRequest extends ActionRequest implements ToXContentObject {

    private static final String TASK_ID_FIELD = "task_id";
    private static final String SIZE_FIELD = "size";
    private static final String SPLIT_BY_FIELD = "split_by";
    private static final String FILTER_BY_FIELD = "filter_by";
    private static final String BUILD_IN_QUERY_FIELD = "build_in_query";
    private static final String ENTITY_FIELD = "entity";
    private static final String THRESHOLD_FIELD = "threshold";
    private static final String FILTER_QUERY_FIELD = "filter_query";
    public static final String SUBAGGREGATIONS_FIELD = "subaggregations";
    private static final String FORECAST_FROM_FIELD = "forecast_from";

    private String forecasterId;
    private String taskId;
    private boolean historical;
    private Integer size;
    private List<String> splitBy;
    private FilterBy filterBy;
    private QueryBuilder buildInQuery;
    private Entity entity;
    private Float threshold;
    private QueryBuilder filterQuery;
    private List<Subaggregation> subaggregations;
    private Instant forecastFrom;

    public SearchTopForecastResultRequest(StreamInput in) throws IOException {
        super(in);
        forecasterId = in.readOptionalString();
        taskId = in.readOptionalString();
        historical = in.readBoolean();
        size = in.readOptionalInt();
        splitBy = in.readOptionalStringList();
        if (in.readBoolean()) {
            filterBy = in.readEnum(FilterBy.class);
        } else {
            filterBy = null;
        }
        if (in.readBoolean()) {
            buildInQuery = in.readNamedWriteable(QueryBuilder.class);
        } else {
            buildInQuery = null;
        }
        if (in.readBoolean()) {
            this.entity = new Entity(in);
        } else {
            this.entity = null;
        }
        threshold = in.readOptionalFloat();
        if (in.readBoolean()) {
            filterQuery = in.readNamedWriteable(QueryBuilder.class);
        } else {
            filterQuery = null;
        }
        if (in.readBoolean()) {
            subaggregations = in.readList(Subaggregation::new);
        } else {
            subaggregations = null;
        }
        forecastFrom = in.readOptionalInstant();
    }

    public SearchTopForecastResultRequest(
        String detectorId,
        String taskId,
        boolean historical,
        Integer size,
        List<String> splitBy,
         FilterBy filterBy,
         QueryBuilder buildInQuery,
         Entity entity,
         Float threshold,
         QueryBuilder filterQuery,
         List<Subaggregation> subaggregations,
         Instant forecastFrom
    ) {
        super();
        this.forecasterId = detectorId;
        this.taskId = taskId;
        this.historical = historical;
        this.size = size;
        this.splitBy = splitBy;
        this.filterBy = filterBy;
        this.buildInQuery = buildInQuery;
        this.entity = entity;
        this.threshold = threshold;
        this.filterQuery = filterQuery;
        this.subaggregations = subaggregations;
        this.forecastFrom = forecastFrom;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean getHistorical() {
        return historical;
    }

    public Integer getSize() {
        return size;
    }

    public String getForecasterId() {
        return forecasterId;
    }

    public List<String> getSplitBy() {
        return splitBy;
    }

    public FilterBy getFilterBy() {
        return filterBy;
    }

    public QueryBuilder getBuildInQuery() {
        return buildInQuery;
    }

    public Entity getEntity() {
        return entity;
    }

    public Float getThreshold() {
        return threshold;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    public List<Subaggregation> getSubaggregations() {
        return subaggregations;
    }

    public Instant getForecastFrom() {
        return forecastFrom;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setForecasterId(String forecasterId) {
        this.forecasterId = forecasterId;
    }

    public void setHistorical(boolean historical) {
        this.historical = historical;
    }

    public void setSplitBy(List<String> splitBy) {
        this.splitBy = splitBy;
    }

    public void setFilterBy(FilterBy filterBy) {
        this.filterBy = filterBy;
    }

    public void setBuildInQuery(QueryBuilder buildInQuery) {
        this.buildInQuery = buildInQuery;
    }

    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    public void setThreshold(Float threshold) {
        this.threshold = threshold;
    }

    public void setFilterQuery(QueryBuilder filterQuery) {
        this.filterQuery = filterQuery;
    }

    public void setSubaggregations(List<Subaggregation> subaggregations) {
        this.subaggregations = subaggregations;
    }

    public void setForecastFrom(Instant forecastFrom) {
        this.forecastFrom = forecastFrom;
    }

    public static SearchTopForecastResultRequest parse(XContentParser parser, String forecasterId, boolean historical) throws IOException {
        String taskId = null;
        Integer size = null;
        List<String> splitBy = null;
        FilterBy filterBy = null;
        QueryBuilder buildInQuery = null;
        Entity entity = null;
        Float threshold = null;
        QueryBuilder filterQuery = null;
        List<Subaggregation> subaggregations = new ArrayList<>();
        Instant forecastFrom = null;

        // "forecasterId" and "historical" params come from the original API path, not in the request body
        // and therefore don't need to be parsed
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case SIZE_FIELD:
                    size = parser.intValue();
                    break;
                case SPLIT_BY_FIELD:
                    splitBy = Arrays.asList(parser.text().split(","));
                    break;
                case FILTER_BY_FIELD:
                    filterBy = FilterBy.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case BUILD_IN_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    try {
                        buildInQuery = parseInnerQueryBuilder(parser);
                    } catch (ParsingException | XContentParseException e) {
                        throw new IllegalArgumentException(
                            "Built-in query error in built-in query: " + e.getMessage()
                        );
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case THRESHOLD_FIELD:
                    threshold = parser.floatValue();
                    break;
                case FILTER_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    try {
                        filterQuery = parseInnerQueryBuilder(parser);
                    } catch (ParsingException | XContentParseException e) {
                        throw new ValidationException(
                            "Custom query error in data filter: " + e.getMessage(),
                            ValidationIssueType.FILTER_QUERY,
                            ValidationAspect.FORECASTER
                        );
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case SUBAGGREGATIONS_FIELD:
                    try {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            subaggregations.add(Subaggregation.parse(parser));
                        }
                    } catch (Exception e) {
                        if (e instanceof ParsingException || e instanceof XContentParseException) {
                            throw new ValidationException(
                                "Custom query error: " + e.getMessage(),
                                ValidationIssueType.SUBAGGREGATION,
                                ValidationAspect.FORECASTER
                            );
                        }
                        throw e;
                    }
                    break;
                case FORECAST_FROM_FIELD:
                    forecastFrom = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new SearchTopForecastResultRequest(
            forecasterId,
            taskId,
            historical,
            size,
            splitBy,
            filterBy,
            buildInQuery,
            entity,
            threshold,
            filterQuery,
            subaggregations,
            forecastFrom
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // "forecasterId" and "historical" params come from the original API path, not in the request body
        // and therefore don't need to be in the generated json
        builder.field(TASK_ID_FIELD, taskId)
               .field(SPLIT_BY_FIELD, String.join(",", splitBy))
               .field(FILTER_BY_FIELD, filterBy);

        if (size != null) {
            builder.field(SIZE_FIELD, size);
        }
        if (buildInQuery != null) {
            builder.field(BUILD_IN_QUERY_FIELD, buildInQuery);
        }
        if (entity != null) {
            builder.field(ENTITY_FIELD, entity);
        }
        if (threshold != null) {
            builder.field(THRESHOLD_FIELD, threshold);
        }
        if (filterQuery != null) {
            builder.field(FILTER_QUERY_FIELD, filterQuery);
        }
        if (subaggregations != null) {
            builder.field(SUBAGGREGATIONS_FIELD, subaggregations.toArray());
        }
        if (forecastFrom != null) {
            builder.field(FORECAST_FROM_FIELD, forecastFrom.toString());
        }

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(forecasterId);
        out.writeOptionalString(taskId);
        out.writeBoolean(historical);
        out.writeOptionalInt(size);
        out.writeOptionalStringCollection(splitBy);
        if (filterBy == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeEnum(filterBy);
        }
        if (buildInQuery == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeNamedWriteable(buildInQuery);
        }
        if (entity == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            entity.writeTo(out);
        }
        out.writeOptionalFloat(threshold);
        if (filterQuery == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeNamedWriteable(filterQuery);
        }
        if (subaggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(subaggregations);
        }
        out.writeOptionalInstant(forecastFrom);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (forecasterId == null) {
            return addValidationError("Must set forecasterId", null);
        }
        if (filterBy == null) {
            return addValidationError("Must set filterBy", null);
        }
        if (forecastFrom == null) {
            return addValidationError("Must set forecastFrom time with epoch of milliseconds", null);
        }
        return null;
    }
}
