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

package org.opensearch.timeseries.ratelimit;

import java.io.IOException;

import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.timeseries.model.IndexableResult;

public abstract class ResultWriteRequest<ResultType extends IndexableResult> extends QueuedRequest implements Writeable {
    private final ResultType result;
    // If resultIndex is null, result will be stored in default result index.
    private final String resultIndex;

    public ResultWriteRequest(
        long expirationEpochMs,
        String detectorId,
        RequestPriority priority,
        ResultType result,
        String resultIndex
    ) {
        super(expirationEpochMs, detectorId, priority);
        this.result = result;
        this.resultIndex = resultIndex;
    }

    /**
     *
     * @param <T> subclass type
     * @param <R> result type
     * @param expirationEpochMs expiration epoch in milliseconds
     * @param configId config id
     * @param priority request priority
     * @param result result
     * @param resultIndex result index
     * @param clazz The clazz parameter is used to pass the class object of the desired subtype, which allows us to perform a dynamic cast to T and return the correctly-typed instance.
     * @return
     */
    public static <T extends ResultWriteRequest<R>, R extends IndexableResult> T create(
            long expirationEpochMs,
            String configId,
            RequestPriority priority,
            IndexableResult result,
            String resultIndex,
            Class<T> clazz
        ) {
            if (result instanceof AnomalyResult) {
                return clazz.cast(new ADResultWriteRequest(expirationEpochMs, configId, priority, (AnomalyResult) result, resultIndex));
            } else if (result instanceof ForecastResult) {
                return clazz.cast(new ForecastResultWriteRequest(expirationEpochMs, configId, priority, (ForecastResult) result, resultIndex));
            } else {
                throw new IllegalArgumentException("Unsupported result type");
            }
        }

    public ResultWriteRequest(StreamInput in, Writeable.Reader<ResultType> resultReader) throws IOException {
        this.result = resultReader.read(in);
        this.resultIndex = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        result.writeTo(out);
        out.writeOptionalString(resultIndex);
    }

    public ResultType getResult() {
        return result;
    }

    public String getResultIndex() {
        return resultIndex;
    }
}
