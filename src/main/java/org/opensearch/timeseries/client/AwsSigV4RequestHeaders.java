/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Locale;
import java.util.Objects;

import org.apache.hc.core5.http.Header;
import org.opensearch.client.Request;

public final class AwsSigV4RequestHeaders {
    public static final String CONTENT_SHA256 = "x-amz-content-sha256";
    public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

    private AwsSigV4RequestHeaders() {}

    public static void addUnsignedPayloadHeader(Request request) {
        Objects.requireNonNull(request, "request must not be null");
        if (hasUnsignedPayloadHeader(request)) {
            return;
        }
        request.setOptions(request.getOptions().toBuilder().addHeader(CONTENT_SHA256, UNSIGNED_PAYLOAD));
    }

    public static boolean hasUnsignedPayloadHeader(org.apache.hc.core5.http.HttpRequest request) {
        Header header = request.getFirstHeader(CONTENT_SHA256);
        return header != null && UNSIGNED_PAYLOAD.equals(header.getValue());
    }

    static boolean hasUnsignedPayloadHeader(Request request) {
        for (Header header : request.getOptions().getHeaders()) {
            if (CONTENT_SHA256.equals(header.getName().toLowerCase(Locale.ROOT)) && UNSIGNED_PAYLOAD.equals(header.getValue())) {
                return true;
            }
        }
        return false;
    }
}
