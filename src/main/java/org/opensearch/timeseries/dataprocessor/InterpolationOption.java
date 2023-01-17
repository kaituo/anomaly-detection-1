/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class InterpolationOption implements Writeable, ToXContent {
    private static final Logger LOG = LogManager.getLogger(InterpolationOption.class);

    // field name in toXContent
    public static final String METHOD_FIELD = "method";
    public static final String DEFAULT_FILL_FIELD = "defaultFill";

    private final InterpolationMethod method;
    private final Optional<double[]> defaultFill;

    public InterpolationOption(InterpolationMethod method, Optional<double[]> defaultFill) {
        this.method = method;
        this.defaultFill = defaultFill;
    }

    public InterpolationOption(InterpolationMethod method) {
        this(method, Optional.empty());
    }

    public InterpolationOption(StreamInput in) throws IOException {
        this.method = in.readEnum(InterpolationMethod.class);
        if (in.readBoolean()) {
            this.defaultFill = Optional.of(in.readDoubleArray());
        } else {
            this.defaultFill = Optional.empty();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(method);
        if (defaultFill.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDoubleArray(defaultFill.get());
        }
    }

    public static InterpolationOption parse(XContentParser parser) throws IOException {
        InterpolationMethod method = InterpolationMethod.ZERO;
        List<Double> defaultFill = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
            case METHOD_FIELD:
                method = InterpolationMethod.valueOf(parser.currentName().toUpperCase(Locale.ROOT));
                break;
            case DEFAULT_FILL_FIELD:
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    defaultFill.add(parser.doubleValue());
                }
                break;
            default:
                break;
            }
        }
        return new InterpolationOption(method, Optional.of(defaultFill.stream().mapToDouble(Double::doubleValue).toArray()));
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        xContentBuilder.field(METHOD_FIELD, method);

        if (!defaultFill.isEmpty()) {
            builder.array(DEFAULT_FILL_FIELD, defaultFill);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InterpolationOption other = (InterpolationOption) o;
        return method == other.method && (defaultFill.isEmpty() ? other.defaultFill.isEmpty() : Arrays.equals(defaultFill.get(), other.defaultFill.get()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, (defaultFill.isEmpty() ? 0 : Arrays.hashCode(defaultFill.get())));
    }

    public InterpolationMethod getMethod() {
        return method;
    }

    public Optional<double[]> getDefaultFill() {
        return defaultFill;
    }
}
