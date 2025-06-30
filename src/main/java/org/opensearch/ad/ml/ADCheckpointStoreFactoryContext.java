/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.time.Clock;
import java.util.Objects;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.client.Client;

import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;

/**
 * Construction context for AD checkpoint store factories.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Carries the single-tenant client through to the index-backed checkpoint store factory.")
public class ADCheckpointStoreFactoryContext {
    private final Settings settings;
    private final ClusterService clusterService;
    private final Client client;
    private final ClientUtil clientUtil;
    private final Gson gson;
    private final RandomCutForestMapper rcfMapper;
    private final V1JsonToV3StateConverter converter;
    private final ThresholdedRandomCutForestMapper trcfMapper;
    private final Schema<ThresholdedRandomCutForestState> trcfSchema;
    private final Class<? extends ThresholdingModel> thresholdingModelClass;
    private final ADDelegatingDataManagement dataManagement;
    private final int maxCheckpointBytes;
    private final GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private final int serializeRCFBufferSize;
    private final double anomalyRate;
    private final Clock clock;
    /**
     * Optional node {@link ThreadPool}. The Neo-backed checkpoint store uses it to snapshot the
     * caller's {@code ThreadContext} (signing-factory transient, tenant headers, etc.) before each
     * async Coral call so the listener — which runs on the fork-join common pool — can restore them
     * before invoking downstream code that depends on those transients. Index-backed stores ignore it.
     */
    private final ThreadPool threadPool;

    public ADCheckpointStoreFactoryContext(
        Settings settings,
        ClusterService clusterService,
        Client client,
        ClientUtil clientUtil,
        Gson gson,
        RandomCutForestMapper rcfMapper,
        V1JsonToV3StateConverter converter,
        ThresholdedRandomCutForestMapper trcfMapper,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        ADDelegatingDataManagement dataManagement,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        double anomalyRate,
        Clock clock,
        ThreadPool threadPool
    ) {
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.clusterService = clusterService;
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.clientUtil = Objects.requireNonNull(clientUtil, "clientUtil must not be null");
        this.gson = Objects.requireNonNull(gson, "gson must not be null");
        this.rcfMapper = Objects.requireNonNull(rcfMapper, "rcfMapper must not be null");
        this.converter = Objects.requireNonNull(converter, "converter must not be null");
        this.trcfMapper = Objects.requireNonNull(trcfMapper, "trcfMapper must not be null");
        this.trcfSchema = Objects.requireNonNull(trcfSchema, "trcfSchema must not be null");
        this.thresholdingModelClass = Objects.requireNonNull(thresholdingModelClass, "thresholdingModelClass must not be null");
        this.dataManagement = Objects.requireNonNull(dataManagement, "dataManagement must not be null");
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.serializeRCFBufferPool = Objects.requireNonNull(serializeRCFBufferPool, "serializeRCFBufferPool must not be null");
        this.serializeRCFBufferSize = serializeRCFBufferSize;
        this.anomalyRate = anomalyRate;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        // threadPool is intentionally nullable: legacy/test-only flows that don't need
        // ThreadContext snapshotting can pass null.
        this.threadPool = threadPool;
    }

    public Settings getSettings() {
        return settings;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public Client getClient() {
        return client;
    }

    public ClientUtil getClientUtil() {
        return clientUtil;
    }

    public Gson getGson() {
        return gson;
    }

    public RandomCutForestMapper getRcfMapper() {
        return rcfMapper;
    }

    public V1JsonToV3StateConverter getConverter() {
        return converter;
    }

    public ThresholdedRandomCutForestMapper getTrcfMapper() {
        return trcfMapper;
    }

    public Schema<ThresholdedRandomCutForestState> getTrcfSchema() {
        return trcfSchema;
    }

    public Class<? extends ThresholdingModel> getThresholdingModelClass() {
        return thresholdingModelClass;
    }

    public ADDelegatingDataManagement getDataManagement() {
        return dataManagement;
    }

    public int getMaxCheckpointBytes() {
        return maxCheckpointBytes;
    }

    public GenericObjectPool<LinkedBuffer> getSerializeRCFBufferPool() {
        return serializeRCFBufferPool;
    }

    public int getSerializeRCFBufferSize() {
        return serializeRCFBufferSize;
    }

    public double getAnomalyRate() {
        return anomalyRate;
    }

    public Clock getClock() {
        return clock;
    }

    /**
     * @return the node {@link ThreadPool}, or {@code null} when no thread-context propagation is
     *         needed (e.g. unit tests, single-tenant flows). Neo-backed factories pass it into the
     *         checkpoint DAO so async Coral callbacks can restore the caller's {@code ThreadContext}.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }
}
