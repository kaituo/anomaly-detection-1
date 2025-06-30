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

package org.opensearch.ad.ml;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.ml.CheckpointCodec;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Delegates checkpoint operations to either the local index-backed DAO or the S3-backed DAO,
 * depending on whether AD multi-tenancy is enabled.
 */
public class DelegatingADCheckpointStore implements ADCheckpointStore {

    private static final Logger LOG = LogManager.getLogger(DelegatingADCheckpointStore.class);

    private final ADCheckpointStore indexStore;
    private final ADCheckpointStore s3Store;
    private final boolean s3Available;
    private final AtomicReference<ADCheckpointStore> current;
    private final AtomicBoolean missingS3WarningLogged;

    /**
     * Creates a delegating checkpoint store.
     *
     * @param indexStore store backed by the dedicated AD checkpoint index
     * @param s3Store store backed by S3; may be {@code null} when S3 is not configured
     * @param clusterService cluster service (optional, used for settings listener)
     */
    public DelegatingADCheckpointStore(ADCheckpointStore indexStore, ADCheckpointStore s3Store, ClusterService clusterService) {
        this.indexStore = Objects.requireNonNull(indexStore, "indexStore must not be null");
        this.s3Store = s3Store;
        this.s3Available = s3Store != null;
        this.missingS3WarningLogged = new AtomicBoolean(false);
        boolean multiTenancyEnabled = clusterService != null
            && AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(clusterService.getSettings());
        this.current = new AtomicReference<>(selectDelegate(multiTenancyEnabled));
    }

    private ADCheckpointStore selectDelegate(boolean multiTenancyEnabled) {
        if (multiTenancyEnabled) {
            if (s3Available) {
                return s3Store;
            }
            if (missingS3WarningLogged.compareAndSet(false, true)) {
                LOG
                    .warn(
                        "AD multi-tenancy is enabled but S3 checkpoint storage is not configured; falling back to index-based checkpoints."
                    );
            }
        }
        return indexStore;
    }

    private ADCheckpointStore delegate() {
        return current.get();
    }

    @Override
    public void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener) {
        delegate().deleteModelCheckpoint(config, modelId, listener);
    }

    @Override
    public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
        delegate().batchWrite(request, listener);
    }

    @Override
    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        delegate().batchRead(request, listener);
    }

    @Override
    public void deleteModelCheckpointByConfigId(String tenantId, String configId) {
        delegate().deleteModelCheckpointByConfigId(tenantId, configId);
    }

    @Override
    public CheckpointCodec<ThresholdedRandomCutForest> getCodec() {
        return delegate().getCodec();
    }

    @Override
    public void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener) {
        delegate().putTRCFCheckpoint(modelId, forest, listener);
    }

    @Override
    public void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener) {
        delegate().putThresholdCheckpoint(modelId, threshold, listener);
    }

    @Override
    public void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener) {
        delegate().getTRCFModel(modelId, listener);
    }

    @Override
    public void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener) {
        delegate().getThresholdModel(modelId, listener);
    }
}
