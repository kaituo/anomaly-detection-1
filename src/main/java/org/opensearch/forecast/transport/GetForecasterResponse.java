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


package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.rest.RestStatus;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class GetForecasterResponse extends ActionResponse implements ToXContentObject {

    public static final String DETECTOR_PROFILE = "forecasterProfile";
    public static final String ENTITY_PROFILE = "entityProfile";
    private String id;
    private long version;
    private long primaryTerm;
    private long seqNo;
    private Forecaster forecaster;
    // TODO: add job and real time task
    // private AnomalyDetectorJob adJob;
    // private ADTask realtimeAdTask;
    // TODO: add historical task
    // private ADTask historicalAdTask;
    private RestStatus restStatus;
    // TODO: add forecaster and entity profile
    //private DetectorProfile detectorProfile;
    //private EntityProfile entityProfile;
    // private boolean profileResponse;
    // private boolean returnJob;
    // private boolean returnTask;

    public GetForecasterResponse(StreamInput in) throws IOException {
        super(in);

        id = in.readString();
        version = in.readLong();
        primaryTerm = in.readLong();
        seqNo = in.readLong();
        restStatus = in.readEnum(RestStatus.class);
        forecaster = new Forecaster(in);
    }

    public GetForecasterResponse(
        String id,
        long version,
        long primaryTerm,
        long seqNo,
        Forecaster forecaster,
        RestStatus restStatus
    ) {
        this.id = id;
        this.version = version;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.forecaster = forecaster;
        this.restStatus = restStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeLong(primaryTerm);
        out.writeLong(seqNo);
        out.writeEnum(restStatus);
        forecaster.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RestHandlerUtils._ID, id);
            builder.field(RestHandlerUtils._VERSION, version);
            builder.field(RestHandlerUtils._PRIMARY_TERM, primaryTerm);
            builder.field(RestHandlerUtils._SEQ_NO, seqNo);
            builder.field(RestHandlerUtils.REST_STATUS, restStatus);
            builder.field(RestHandlerUtils.FORECASTER, forecaster);
            builder.endObject();
        return builder;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }
}
