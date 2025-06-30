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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class SearchConfigInfoRequest extends ActionRequest {

    private String name;
    private String rawPath;
    private String tenantId;

    public SearchConfigInfoRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
        rawPath = in.readString();
        tenantId = in.readOptionalString();
    }

    public SearchConfigInfoRequest(String name, String rawPath, String tenantId) throws IOException {
        super();
        this.name = name;
        this.rawPath = rawPath;
        this.tenantId = tenantId;
    }

    public SearchConfigInfoRequest(String name, String rawPath) throws IOException {
        this(name, rawPath, null);
    }

    public String getName() {
        return name;
    }

    public String getRawPath() {
        return rawPath;
    }

    public String getTenantId() {
        return tenantId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeString(rawPath);
        out.writeOptionalString(tenantId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
