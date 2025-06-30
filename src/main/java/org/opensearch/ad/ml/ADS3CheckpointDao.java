/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_S3_CHECKPOINT_BUCKET;

import java.time.Clock;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.ml.S3CheckpointDao;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;

/**
 * AD-specific S3 checkpoint DAO that initializes the S3 async client and bucket information
 * using AD settings.
 */
public class ADS3CheckpointDao extends S3CheckpointDao<ThresholdedRandomCutForest> implements ADCheckpointStore {

    private static String resolveBucket(Settings settings) {
        String bucket = AD_S3_CHECKPOINT_BUCKET.get(settings);
        if (Strings.isEmpty(bucket)) {
            throw new TimeSeriesException("S3 checkpoint bucket setting must be configured");
        }
        return bucket;
    }

    private static ADCheckpointCodec createCodec(
        int maxCheckpointBytes,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        ThresholdedRandomCutForestMapper trcfMapper,
        V1JsonToV3StateConverter converter,
        Gson gson,
        RandomCutForestMapper mapper,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        double anomalyRate,
        Clock clock,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        ADIndexManagement indexUtil
    ) {
        return new ADCheckpointCodec(
            maxCheckpointBytes,
            trcfSchema,
            trcfMapper,
            converter,
            gson,
            mapper,
            thresholdingModelClass,
            anomalyRate,
            clock,
            serializeRCFBufferPool,
            serializeRCFBufferSize,
            indexUtil
        );
    }

    /**
     * Creates an AD S3 checkpoint DAO.
     *
     * @param settings cluster settings
     * @param region S3 region
     * @param maxCheckpointBytes max checkpoint bytes
     * @param trcfSchema schema for TRCF state
     * @param trcfMapper mapper for TRCF
     * @param converter converter for TRCF
     * @param gson gson for JSON serialization
     * @param mapper mapper for RCF
     * @param thresholdingModelClass thresholding model class
     * @param anomalyRate anomaly rate
     * @param clock clock used for timestamp generation
     * @param serializeRCFBufferPool buffer pool for RCF serialization
     * @param serializeRCFBufferSize buffer size for RCF serialization
     * @param indexUtil index util
     */
    public ADS3CheckpointDao(
        Settings settings,
        int maxCheckpointBytes,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        ThresholdedRandomCutForestMapper trcfMapper,
        V1JsonToV3StateConverter converter,
        Gson gson,
        RandomCutForestMapper mapper,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        double anomalyRate,
        Clock clock,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        ADIndexManagement indexUtil
    ) {
        super(
            resolveBucket(settings),
            TimeSeriesSettings.REGION.get(settings),
            createCodec(
                maxCheckpointBytes,
                trcfSchema,
                trcfMapper,
                converter,
                gson,
                mapper,
                thresholdingModelClass,
                anomalyRate,
                clock,
                serializeRCFBufferPool,
                serializeRCFBufferSize,
                indexUtil
            )
        );
    }

    @Override
    public void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener) {
        // old code for backward compatibility. Don't need it in multitenant mode as all models are stored in the new way.
        throw new UnsupportedOperationException("putTRCFCheckpoint is not supported for ADS3CheckpointDao");
    }

    @Override
    public void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener) {
        // old code for backward compatibility. Don't need it in multitenant mode as all models are stored in the new way.
        throw new UnsupportedOperationException("putThresholdCheckpoint is not supported for ADS3CheckpointDao");
    }

    @Override
    public void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener) {
        throw new UnsupportedOperationException("getTRCFModel is not supported for ADS3CheckpointDao");
    }

    @Override
    public void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener) {
        throw new UnsupportedOperationException("getThresholdModel is not supported for ADS3CheckpointDao");
    }

}