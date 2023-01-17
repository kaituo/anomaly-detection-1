/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.function;

@FunctionalInterface
public interface BiCheckedFunction<T, R, F, E extends Exception> {
    R apply(T t, F f) throws E;
}
