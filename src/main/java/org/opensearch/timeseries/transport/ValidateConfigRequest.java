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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;

public class ValidateConfigRequest extends ActionRequest {

    private final AnalysisType context;
    private final Config config;
    private final String validationType;
    private final Integer maxSingleEntityAnomalyDetectors;
    private final Integer maxMultiEntityAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;
    // added during refactoring for forecasting. It is fine we add a new field
    // since the request is handled by the same node.
    private final Integer maxCategoricalFields;

    public ValidateConfigRequest(StreamInput in) throws IOException {
        super(in);
        context = in.readEnum(AnalysisType.class);
        if (context.isAD()) {
            config = new AnomalyDetector(in);
        } else if (context.isForecast()) {
            config = new Forecaster(in);
        } else {
            throw new UnsupportedOperationException("This method is not supported");
        }

        validationType = in.readString();
        maxSingleEntityAnomalyDetectors = in.readInt();
        maxMultiEntityAnomalyDetectors = in.readInt();
        maxAnomalyFeatures = in.readInt();
        requestTimeout = in.readTimeValue();
        maxCategoricalFields = in.readInt();
    }

    public ValidateConfigRequest(
        AnalysisType context,
        Config config,
        String validationType,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        TimeValue requestTimeout,
        Integer maxCategoricalFields
    ) {
        this.context = context;
        this.config = config;
        this.validationType = validationType;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.requestTimeout = requestTimeout;
        this.maxCategoricalFields = maxCategoricalFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(context);
        config.writeTo(out);
        out.writeString(validationType);
        out.writeInt(maxSingleEntityAnomalyDetectors);
        out.writeInt(maxMultiEntityAnomalyDetectors);
        out.writeInt(maxAnomalyFeatures);
        out.writeTimeValue(requestTimeout);
        out.writeInt(maxCategoricalFields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public Config getConfig() {
        return config;
    }

    public String getValidationType() {
        return validationType;
    }

    public Integer getMaxSingleEntityAnomalyDetectors() {
        return maxSingleEntityAnomalyDetectors;
    }

    public Integer getMaxMultiEntityAnomalyDetectors() {
        return maxMultiEntityAnomalyDetectors;
    }

    public Integer getMaxAnomalyFeatures() {
        return maxAnomalyFeatures;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    public Integer getMaxCategoricalFields() {
        return maxCategoricalFields;
    }
}
