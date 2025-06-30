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

import java.io.IOException;
import java.lang.reflect.Type;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointCodec;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Entity;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

public final class ADCheckpointCodec extends CheckpointCodec<ThresholdedRandomCutForest> {
    private static final Logger logger = LogManager.getLogger(ADCheckpointCodec.class);
    private final JsonParser parser;
    private final int maxCheckpointBytes;
    private final Schema<ThresholdedRandomCutForestState> trcfSchema;
    private final ThresholdedRandomCutForestMapper trcfMapper;
    // For further reference v1, v2 and v3 refer to the different variations of RCF models
    // used by AD. v1 was originally used with the launch of OS 1.0. We later converted to v2
    // which included changes requiring a specific converter from v1 to v2 for BWC.
    // v2 models are created by RCF-3.0-rc1 which can be found on maven central.
    // v3 is the latest model version form RCF introduced by RCF-3.0-rc2.
    // Although this version has a converter method for v2 to v3, after BWC testing it was decided that
    // an explicit use of the converter won't be needed as the changes between the models are indeed BWC.
    private V1JsonToV3StateConverter converter;
    private Gson gson;
    private RandomCutForestMapper mapper;
    private Class<? extends ThresholdingModel> thresholdingModelClass;
    // Use TypeToken to properly deserialize the double array
    private final Type doubleArrayType;
    // anomaly rate
    private double anomalyRate;
    private Clock clock;
    private ADDelegatingDataManagement dataManagement;

    public ADCheckpointCodec(
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
        ADDelegatingDataManagement dataManagement
    ) {
        super(serializeRCFBufferPool, serializeRCFBufferSize);
        this.parser = new JsonParser();
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.trcfSchema = trcfSchema;
        this.trcfMapper = trcfMapper;
        this.converter = converter;
        this.gson = gson;
        this.mapper = mapper;
        this.thresholdingModelClass = thresholdingModelClass;
        this.doubleArrayType = new TypeToken<double[][]>() {
        }.getType();
        this.anomalyRate = anomalyRate;
        this.clock = clock;
        this.dataManagement = dataManagement;
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    ThresholdedRandomCutForest toTrcf(String checkpoint) {
        ThresholdedRandomCutForest trcf = null;
        if (checkpoint != null && !checkpoint.isEmpty()) {
            try {
                byte[] bytes = Base64.getDecoder().decode(checkpoint);
                ThresholdedRandomCutForestState state = trcfSchema.newMessage();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    ProtostuffIOUtil.mergeFrom(bytes, state, trcfSchema);
                    return null;
                });
                trcf = trcfMapper.toModel(state);
            } catch (RuntimeException e) {
                logger.info("checkpoint to restore: " + checkpoint);
                logger.error("Failed to deserialize TRCF model", e);
            }
        }
        return trcf;
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    public Optional<RandomCutForest> deserializeRCFModel(String checkpoint, String modelId) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(AccessController.doPrivileged((PrivilegedAction<RandomCutForest>) () -> {
            try {
                RandomCutForestState state = converter.convert(checkpoint, Precision.FLOAT_32);
                return mapper.toModel(state);
            } catch (Exception e) {
                logger.error("Unexpected error when deserializing " + modelId, e);
                return null;
            }
        }));
    }

    private Deque<Sample> processSampleQueue(JsonObject json, Map<String, Object> checkpoint, String modelId) {
        Deque<Sample> sampleQueue = new ArrayDeque<>();
        if (json.has(CommonName.ENTITY_SAMPLE)) {
            double[][] samplesArray = this.gson.fromJson(json.getAsJsonArray(CommonName.ENTITY_SAMPLE), doubleArrayType);
            // this branch exists for bwc. Since we didn't record start and end time, we have to give a default 0.
            Arrays
                .stream(samplesArray)
                .map(sampleArray -> new Sample(sampleArray, Instant.ofEpochMilli(0), Instant.ofEpochMilli(0)))
                .forEach(sampleQueue::add);
        } else {
            sampleQueue = loadSampleQueue(checkpoint, modelId);
        }
        return sampleQueue;
    }

    private Instant loadLastProcessedDataEndTime(Map<String, Object> checkpoint, String modelId) {
        Object value = checkpoint.get(ModelState.LAST_PROCESSED_DATA_END_TIME_KEY);
        if (value == null) {
            return Instant.MIN;
        }
        try {
            if (value instanceof Number number) {
                return Instant.ofEpochMilli(number.longValue());
            }
            String valueString = value.toString();
            if (valueString.isBlank()) {
                return Instant.MIN;
            }
            try {
                return Instant.ofEpochMilli(Long.parseLong(valueString));
            } catch (NumberFormatException ignored) {
                return Instant.parse(valueString);
            }
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Failed to parse last processed data end time for [{}]: [{}]", modelId, value), e);
            return Instant.MIN;
        }
    }

    public Optional<ThresholdedRandomCutForest> convertToTRCF(RandomCutForest rcf, Optional<ThresholdingModel> kllThreshold) {
        if (rcf == null) {
            return Optional.empty();
        }
        // if there is no threshold model (e.g., threshold model is deleted by HourlyCron), we are gonna
        // start with empty list of rcf scores
        List<Double> scores = new ArrayList<>();
        if (kllThreshold.isPresent()) {
            scores = kllThreshold.get().extractScores();
        }
        // last parameter is lastShingledInput. Since we don't know it, use all 0 double array
        return Optional.of(new ThresholdedRandomCutForest(rcf, anomalyRate, scores, new double[rcf.getDimensions()]));
    }

    /**
     * Load json checkpoint into models
     *
     * @param checkpoint json checkpoint contents
     * @param modelId Model Id
     * @return a pair of entity model and its last checkpoint time; or empty if
     *  the raw checkpoint is too large
     */
    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    public ModelState<ThresholdedRandomCutForest> fromEntityModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId,
        String tenantId
    ) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<ModelState<ThresholdedRandomCutForest>>) () -> {
                Object modelObj = checkpoint.get(CommonName.FIELD_MODELV2);
                if (modelObj == null) {
                    // in case there is old -format checkpoint
                    modelObj = checkpoint.get(CommonName.FIELD_MODEL);
                }
                if (modelObj == null) {
                    logger.warn(new ParameterizedMessage("Empty model for [{}]", modelId));
                    return null;
                }
                String model = (String) modelObj;
                if (model.length() > maxCheckpointBytes) {
                    logger.warn(new ParameterizedMessage("[{}]'s model too large: [{}] bytes", modelId, model.length()));
                    return null;
                }
                JsonObject json = parser.parse(model).getAsJsonObject();
                ThresholdedRandomCutForest trcf = null;

                if (json.has(ADCommonName.ENTITY_TRCF)) {
                    trcf = toTrcf(json.getAsJsonPrimitive(ADCommonName.ENTITY_TRCF).getAsString());
                } else {
                    Optional<RandomCutForest> rcf = Optional.empty();
                    Optional<ThresholdingModel> threshold = Optional.empty();
                    if (json.has(ADCommonName.ENTITY_RCF)) {
                        String serializedRCF = json.getAsJsonPrimitive(ADCommonName.ENTITY_RCF).getAsString();
                        rcf = deserializeRCFModel(serializedRCF, modelId);
                    }
                    if (json.has(ADCommonName.ENTITY_THRESHOLD)) {
                        // verified, don't need privileged call to get permission
                        threshold = Optional
                            .ofNullable(
                                this.gson
                                    .fromJson(json.getAsJsonPrimitive(ADCommonName.ENTITY_THRESHOLD).getAsString(), thresholdingModelClass)
                            );
                    }

                    if (rcf.isPresent()) {
                        Optional<ThresholdedRandomCutForest> convertedTRCF = convertToTRCF(rcf.get(), threshold);
                        // if checkpoint is corrupted (e.g., some unexpected checkpoint when we missed
                        // the mark in backward compatibility), we are not gonna load the model part
                        // the model will have to use live data to initialize
                        if (convertedTRCF.isPresent()) {
                            trcf = convertedTRCF.get();
                        }
                    }
                }

                Deque<Sample> sampleQueue = processSampleQueue(json, checkpoint, modelId);

                String lastCheckpointTimeString = (String) (checkpoint.get(CommonName.TIMESTAMP));
                Instant timestamp = Instant.parse(lastCheckpointTimeString);
                Entity entity = null;
                Object serializedEntity = checkpoint.get(CommonName.ENTITY_KEY);
                if (serializedEntity != null) {
                    try {
                        entity = Entity.fromJsonArray(serializedEntity);
                    } catch (Exception e) {
                        logger.error(new ParameterizedMessage("fail to parse entity", serializedEntity), e);
                    }
                }

                ModelState<ThresholdedRandomCutForest> modelState = new ModelState<ThresholdedRandomCutForest>(
                    trcf,
                    modelId,
                    configId,
                    tenantId,
                    ModelManager.ModelType.TRCF.getName(),
                    clock,
                    0,
                    Optional.ofNullable(entity),
                    sampleQueue
                );
                modelState.setLastCheckpointTime(timestamp);
                modelState.setLastProcessedDataEndTime(loadLastProcessedDataEndTime(checkpoint, modelId));
                return modelState;
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint " + modelId, e);
            // checkpoint corrupted (e.g., a checkpoint not recognized by current code
            // due to bugs). Better redo training.
            return null;
        }
    }

    /**
     * Serialized an EntityModel
     * @param model input model
     * @param modelId model id
     * @return serialized string
     */
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    public Optional<String> toCheckpoint(ThresholdedRandomCutForest model, String modelId) {
        return AccessController.doPrivileged((PrivilegedAction<Optional<String>>) () -> {
            if (model == null) {
                logger.warn("Empty model");
                return Optional.empty();
            }
            try {
                JsonObject json = new JsonObject();
                if (model != null) {
                    json.addProperty(ADCommonName.ENTITY_TRCF, toCheckpoint(model));
                }
                // if json is empty, it will be an empty Json string {}. No need to save it on disk.
                return json.entrySet().isEmpty() ? Optional.empty() : Optional.ofNullable(gson.toJson(json));
            } catch (Exception ex) {
                logger.warn(new ParameterizedMessage("fail to generate checkpoint for [{}]", modelId), ex);
            }
            return Optional.empty();
        });
    }

    String toCheckpoint(ThresholdedRandomCutForest trcf) {
        String checkpoint = null;
        Map.Entry<LinkedBuffer, Boolean> result = checkoutOrNewBuffer();
        LinkedBuffer buffer = result.getKey();
        boolean needCheckin = result.getValue();
        try {
            checkpoint = toCheckpoint(trcf, buffer);
        } catch (Exception e) {
            logger.error("Failed to serialize model", e);
            if (needCheckin) {
                try {
                    serializeRCFBufferPool.invalidateObject(buffer);
                    needCheckin = false;
                } catch (Exception x) {
                    logger.warn("Failed to invalidate buffer", x);
                }
                try {
                    checkpoint = toCheckpoint(trcf, LinkedBuffer.allocate(serializeRCFBufferSize));
                } catch (Exception ex) {
                    logger.warn("Failed to generate checkpoint", ex);
                }
            }
        } finally {
            if (needCheckin) {
                try {
                    serializeRCFBufferPool.returnObject(buffer);
                } catch (Exception e) {
                    logger.warn("Failed to return buffer to pool", e);
                }
            }
        }
        return checkpoint;
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    private String toCheckpoint(ThresholdedRandomCutForest trcf, LinkedBuffer buffer) {
        try {
            byte[] bytes = AccessController.doPrivileged((PrivilegedAction<byte[]>) () -> {
                ThresholdedRandomCutForestState trcfState = trcfMapper.toState(trcf);
                return ProtostuffIOUtil.toByteArray(trcfState, trcfSchema, buffer);
            });
            return Base64.getEncoder().encodeToString(bytes);
        } finally {
            buffer.clear();
        }
    }

    /**
     * Prepare for index request using the contents of the given model state
     * @param modelState an entity model state
     * @return serialized JSON map or empty map if the state is too bloated
     * @throws IOException  when serialization fails
     */
    @Override
    public Map<String, Object> toIndexSource(ModelState<ThresholdedRandomCutForest> modelState) throws IOException {
        String modelId = modelState.getModelId();
        Map<String, Object> source = new HashMap<>();

        Optional<ThresholdedRandomCutForest> model = modelState.getModel();
        if (model.isPresent()) {
            ThresholdedRandomCutForest entityModel = model.get();

            Optional<String> serializedModel = toCheckpoint(entityModel, modelId);
            if (!serializedModel.isPresent() || serializedModel.get().length() > maxCheckpointBytes) {
                logger
                    .warn(
                        new ParameterizedMessage(
                            "[{}]'s model is empty or too large: [{}] bytes",
                            modelState.getModelId(),
                            serializedModel.isPresent() ? serializedModel.get().length() : 0
                        )
                    );
                return source;
            }
            source.put(CommonName.FIELD_MODELV2, serializedModel.get());
        }

        Optional<Sample[]> samples = toCheckpoint(modelState.getSamples());
        if (samples.isPresent()) {
            source.put(CommonName.SAMPLE_QUEUE, samples.get());
        }

        // if there are no samples and no model, no need to index as other information are meta data
        if (!source.containsKey(CommonName.SAMPLE_QUEUE) && !source.containsKey(CommonName.FIELD_MODELV2)) {
            return source;
        }

        String detectorId = modelState.getConfigId();
        source.put(ADCommonName.DETECTOR_ID, detectorId);
        // we cannot pass Optional as OpenSearch does not know how to serialize an Optional value

        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        source.put(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD, dataManagement.getSchemaVersion(ADIndex.CHECKPOINT));
        if (!Instant.MIN.equals(modelState.getLastProcessedDataEndTime())) {
            source.put(ModelState.LAST_PROCESSED_DATA_END_TIME_KEY, modelState.getLastProcessedDataEndTime().toEpochMilli());
        }

        Optional<Entity> entity = modelState.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }

        return source;
    }
}
