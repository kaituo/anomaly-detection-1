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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ResultBulkResponse extends ActionResponse {
    public static final String RETRY_REQUESTS_JSON_KEY = "retry_requests";

    private List<IndexRequest> retryRequests;
    private List<IndexRequest> missingResultIndexRequests;

    /**
     *
     * @param retryRequests a list of requests to retry
     */
    public ResultBulkResponse(List<IndexRequest> retryRequests) {
        this(retryRequests, null);
    }

    /**
     *
     * @param retryRequests a list of transiently failed requests to retry
     * @param missingResultIndexRequests a list of requests that failed because the target result index or alias is missing
     */
    public ResultBulkResponse(List<IndexRequest> retryRequests, List<IndexRequest> missingResultIndexRequests) {
        this.retryRequests = retryRequests;
        this.missingResultIndexRequests = missingResultIndexRequests;
    }

    public ResultBulkResponse() {
        this.retryRequests = null;
        this.missingResultIndexRequests = null;
    }

    public ResultBulkResponse(StreamInput in) throws IOException {
        retryRequests = readIndexRequests(in);
        missingResultIndexRequests = readIndexRequests(in);
    }

    private List<IndexRequest> readIndexRequests(StreamInput in) throws IOException {
        int size = in.readInt();
        if (size > 0) {
            List<IndexRequest> requests = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                requests.add(new IndexRequest(in));
            }
            return requests;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeIndexRequests(out, retryRequests);
        writeIndexRequests(out, missingResultIndexRequests);
    }

    private void writeIndexRequests(StreamOutput out, List<IndexRequest> requests) throws IOException {
        if (requests == null || requests.size() == 0) {
            out.writeInt(0);
        } else {
            out.writeInt(requests.size());
            for (IndexRequest result : requests) {
                result.writeTo(out);
            }
        }
    }

    public boolean hasFailures() {
        return hasRetryRequests() || hasMissingResultIndexRequests();
    }

    public boolean hasRetryRequests() {
        return retryRequests != null && retryRequests.size() > 0;
    }

    public boolean hasMissingResultIndexRequests() {
        return missingResultIndexRequests != null && missingResultIndexRequests.size() > 0;
    }

    public Optional<List<IndexRequest>> getRetryRequests() {
        return Optional.ofNullable(retryRequests);
    }

    public Optional<List<IndexRequest>> getMissingResultIndexRequests() {
        return Optional.ofNullable(missingResultIndexRequests);
    }
}
