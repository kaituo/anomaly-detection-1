package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import  org.opensearch.ad.transport.EntityADResultRequest;
import  org.opensearch.forecast.transport.EntityForecastResultRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.timeseries.model.Entity;

public abstract class TimeSeriesEntityResultRequest extends ActionRequest implements ToXContentObject {
    protected String configId;
    // changed from Map<String, double[]> to Map<Entity, double[]>
    protected Map<Entity, double[]> entities;
    // data start/end time epoch
    protected long start;
    protected long end;

    public TimeSeriesEntityResultRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();

        // guarded with version check. Just in case we receive requests from older node where we use String
        // to represent an entity
        this.entities = in.readMap(Entity::new, StreamInput::readDoubleArray);

        this.start = in.readLong();
        this.end = in.readLong();
    }

    public TimeSeriesEntityResultRequest(String configId, Map<Entity, double[]> entities, long start, long end) {
        super();
        this.configId = configId;
        this.entities = entities;
        this.start = start;
        this.end = end;
    }

    public static <T extends TimeSeriesEntityResultRequest> T create(String configId, Map<Entity, double[]> entities, long start, long end, Class<T> clazz) {
        if (clazz.isAssignableFrom(EntityADResultRequest.class)) {
            return clazz.cast(new EntityADResultRequest(configId, entities, start, end));
        } else if (clazz.isAssignableFrom(EntityForecastResultRequest.class)) {
            return clazz.cast(new EntityForecastResultRequest(configId, entities, start, end));
        } else {
            throw new IllegalArgumentException("Unsupported result request type");
        }
    }

    public String getConfigId() {
        return this.configId;
    }

    public Map<Entity, double[]> getEntities() {
        return this.entities;
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.configId);
        // guarded with version check. Just in case we send requests to older node where we use String
        // to represent an entity
        out.writeMap(entities, (s, e) -> e.writeTo(s), StreamOutput::writeDoubleArray);

        out.writeLong(this.start);
        out.writeLong(this.end);
    }
}
