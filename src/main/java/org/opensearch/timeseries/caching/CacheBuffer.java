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

package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class CacheBuffer<RCFModelType extends ThresholdedRandomCutForest, NodeStateType extends NodeState, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>>
    implements
        ExpiringState {

    protected Instant lastUsedTime;
    protected final Clock clock;

    protected final MemoryTracker memoryTracker;
    protected int checkpointIntervalHrs;
    protected final Duration modelTtl;

    // max entities to track per detector
    protected final int MAX_TRACKING_ENTITIES = 1000000;
    // the reserved cache size. So no matter how many entities there are, we will
    // keep the size for minimum capacity entities
    protected int minimumCapacity;
    // memory consumption per entity
    protected final long memoryConsumptionPerModel;
    protected long reservedBytes;
    protected final CheckpointWriterType checkpointWriteQueue;
    protected final CheckpointMaintainerType checkpointMaintainQueue;
    protected final String configId;
    protected final Origin origin;

    public CacheBuffer(
        int minimumCapacity,
        Clock clock,
        MemoryTracker memoryTracker,
        int checkpointIntervalHrs,
        Duration modelTtl,
        long memoryConsumptionPerEntity,
        CheckpointWriterType checkpointWriteQueue,
        CheckpointMaintainerType checkpointMaintainQueue,
        String configId,
        Origin origin
    ) {
        this.lastUsedTime = clock.instant();
        this.clock = clock;
        this.memoryTracker = memoryTracker;
        setCheckpointIntervalHrs(checkpointIntervalHrs);
        this.modelTtl = modelTtl;
        setMinimumCapacity(minimumCapacity);
        this.memoryConsumptionPerModel = memoryConsumptionPerEntity;
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.checkpointMaintainQueue = checkpointMaintainQueue;
        this.configId = configId;
        this.origin = origin;
    }

    public void setMinimumCapacity(int minimumCapacity) {
        if (minimumCapacity < 0) {
            throw new IllegalArgumentException("minimum capacity should be larger than or equal 0");
        }
        this.minimumCapacity = minimumCapacity;
        this.reservedBytes = memoryConsumptionPerModel * minimumCapacity;
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    public void setCheckpointIntervalHrs(int checkpointIntervalHrs) {
        this.checkpointIntervalHrs = checkpointIntervalHrs;
        // 0 can cause java.lang.ArithmeticException: / by zero
        // negative value is meaningless
        if (checkpointIntervalHrs <= 0) {
            this.checkpointIntervalHrs = 1;
        }
    }

    public int getCheckpointIntervalHrs() {
        return checkpointIntervalHrs;
    }

    /**
    *
    * @return reserved bytes by the CacheBuffer
    */
    public long getReservedBytes() {
        return reservedBytes;
    }

    /**
    *
    * @return the estimated number of bytes per entity state
    */
    public long getMemoryConsumptionPerModel() {
        return memoryConsumptionPerModel;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        if (obj instanceof CacheBuffer) {
            @SuppressWarnings("unchecked")
            CacheBuffer<RCFModelType, NodeStateType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType> other =
                (CacheBuffer<RCFModelType, NodeStateType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(configId, other.configId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(configId).toHashCode();
    }

    public String getConfigId() {
        return configId;
    }

    protected abstract void clear();
}
