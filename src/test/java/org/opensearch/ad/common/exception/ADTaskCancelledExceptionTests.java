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

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.ADTaskCancelledException;

public class ADTaskCancelledExceptionTests extends OpenSearchTestCase {

    public void testConstructor() {
        String message = randomAlphaOfLength(5);
        String user = randomAlphaOfLength(5);
        ADTaskCancelledException exception = new ADTaskCancelledException(message, user);
        assertEquals(message, exception.getMessage());
        assertEquals(user, exception.getCancelledBy());
    }
}
