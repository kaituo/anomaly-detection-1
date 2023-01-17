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
import java.time.Instant;
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.forecast.transport.ForecastResultResponse;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;

public abstract class ResultResponse<IndexableResultType extends IndexableResult> extends ActionResponse implements ToXContentObject {

    protected String error;
    protected List<FeatureData> features;
    protected Long rcfTotalUpdates;
    protected Long configIntervalInMinutes;
    protected Boolean isHC;

    public ResultResponse(List<FeatureData> features, String error, Long rcfTotalUpdates, Long configInterval, Boolean isHC) {
        this.error = error;
        this.features = features;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.configIntervalInMinutes = configInterval;
        this.isHC = isHC;
    }

    /**
     * Create an empty result response or when in an erroneous state.
     * @param <T>
     * @param error
     * @param features
     * @param rcfTotalUpdates
     * @param configInterval
     * @param isHC
     * @param clazz
     * @return
     */
    public static <T extends ResultResponse<?>> T create(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        Class<T> clazz
    ) {
        if (clazz.isAssignableFrom(AnomalyResultResponse.class)) {
            return clazz.cast(new AnomalyResultResponse(features, error, rcfTotalUpdates, configInterval, isHC));
        } else if (clazz.isAssignableFrom(ForecastResultResponse.class)) {
            return clazz.cast(new ForecastResultResponse(features, error, rcfTotalUpdates, configInterval, isHC));
        } else {
            throw new IllegalArgumentException("Unsupported result response type");
        }
    }

    /**
     * Leave it as implementation detail in subclass as how to deserialize TimeSeriesResultResponse
     * @param in deserialization stream
     * @throws IOException when deserialization errs
     */
    public ResultResponse(StreamInput in) throws IOException {
        super(in);
    }

    public String getError() {
        return error;
    }

    public List<FeatureData> getFeatures() {
        return features;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public Long getConfigIntervalInMinutes() {
        return configIntervalInMinutes;
    }

    public Boolean isHC() {
        return isHC;
    }

    /**
     *
     * @return whether we should save the response to result index
     */
    public boolean shouldSave() {
        return error != null;
    }

    public abstract List<IndexableResultType> toIndexableResults(
        String configId,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        Integer schemaVersion,
        User user,
        String error
    );
}
