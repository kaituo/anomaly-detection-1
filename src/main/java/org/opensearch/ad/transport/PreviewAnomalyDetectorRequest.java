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

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class PreviewAnomalyDetectorRequest extends ActionRequest implements DocRequest {

    private AnomalyDetector detector;
    private String detectorId;
    private Instant startTime;
    private Instant endTime;
    private String tenantId;

    public PreviewAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detector = new AnomalyDetector(in);
        detectorId = in.readOptionalString();
        startTime = in.readInstant();
        endTime = in.readInstant();
        tenantId = in.readOptionalString();
    }

    public PreviewAnomalyDetectorRequest(AnomalyDetector detector, String detectorId, Instant startTime, Instant endTime, String tenantId)
        throws IOException {
        super();
        this.detector = detector;
        this.detectorId = detectorId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.tenantId = tenantId;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public String getId() {
        return detectorId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public String getTenantId() {
        return tenantId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeInstant(startTime);
        out.writeInstant(endTime);
        out.writeOptionalString(tenantId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String type() {
        return ADCommonName.AD_RESOURCE_TYPE;
    }

    @Override
    public String index() {
        return ADIndex.CONFIG.getIndexName();
    }

    @Override
    public String id() {
        return detectorId;
    }
}
