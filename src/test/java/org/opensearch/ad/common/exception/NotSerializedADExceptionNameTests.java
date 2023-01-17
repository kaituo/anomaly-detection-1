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

package org.opensearch.ad.common.exception;

import java.util.Optional;

import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.ADTaskCancelledException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.NotSerializedADExceptionName;

public class NotSerializedADExceptionNameTests extends OpenSearchTestCase {
    public void testConvertAnomalyDetectionException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new TimeSeriesException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof TimeSeriesException);
    }

    public void testConvertInternalFailure() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new InternalFailure("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof InternalFailure);
    }

    public void testConvertClientException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ClientException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ClientException);
    }

    public void testConvertADTaskCancelledException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ADTaskCancelledException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ADTaskCancelledException);
    }

    public void testConvertDuplicateTaskException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new DuplicateTaskException("")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof DuplicateTaskException);
    }

    public void testConvertADValidationException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ValidationException("", null, null)), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ValidationException);
    }

    public void testUnknownException() {
        Optional<TimeSeriesException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new RuntimeException("")), "");
        assertTrue(!converted.isPresent());
    }
}
