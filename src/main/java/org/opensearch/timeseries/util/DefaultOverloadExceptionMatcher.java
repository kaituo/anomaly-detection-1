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

package org.opensearch.timeseries.util;

public class DefaultOverloadExceptionMatcher implements OverloadExceptionMatcher {

    static final DefaultOverloadExceptionMatcher INSTANCE = new DefaultOverloadExceptionMatcher();

    public DefaultOverloadExceptionMatcher() {}

    @Override
    public boolean isOverloaded(Throwable exception) {
        return false;
    }
}
