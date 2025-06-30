/*
 * SPDX-License-Identifier: Apache-2.0
 * 
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * 
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 * 
 */

package org.opensearch.forecast.ml;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointCodec;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Entity;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.state.RCFCasterMapper;
import com.amazon.randomcutforest.parkservices.state.RCFCasterState;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

public class ForecastCheckpointCodec extends CheckpointCodec<RCFCaster> {
    private static final Logger logger = LogManager.getLogger(ForecastCheckpointCodec.class);

    private final Clock clock;
    private final int maxCheckpointBytes;
    private final RCFCasterMapper mapper;
    private final Schema<RCFCasterState> rcfCasterSchema;
    private final ForecastDelegatingDataManagement dataManagement;

    public ForecastCheckpointCodec(
        Clock clock,
        int maxCheckpointBytes,
        RCFCasterMapper mapper,
        Schema<RCFCasterState> rcfCasterSchema,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        ForecastDelegatingDataManagement dataManagement
    ) {
        super(serializeRCFBufferPool, serializeRCFBufferSize);
        this.clock = clock;
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.mapper = mapper;
        this.rcfCasterSchema = rcfCasterSchema;
        this.dataManagement = dataManagement;
    }

    private Instant loadTimestamp(Map<String, Object> checkpoint, String modelId) {
        String lastCheckpointTimeString = (String) (checkpoint.get(CommonName.TIMESTAMP));
        return Instant.parse(lastCheckpointTimeString);
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    RCFCaster toRCFCaster(String checkpoint) {
        RCFCaster rcfCaster = null;
        if (checkpoint != null && checkpoint.length() > 0) {
            try {
                byte[] bytes = Base64.getDecoder().decode(checkpoint);
                RCFCasterState state = rcfCasterSchema.newMessage();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    ProtostuffIOUtil.mergeFrom(bytes, state, rcfCasterSchema);
                    return null;
                });
                rcfCaster = mapper.toModel(state);
            } catch (RuntimeException e) {
                logger.error("Failed to deserialize RCFCaster model", e);
            }
        }
        return rcfCaster;
    }

    private RCFCaster loadRCFCaster(Map<String, Object> checkpoint, String modelId) {
        String model = (String) checkpoint.get(CommonName.FIELD_MODEL);
        if (model == null || model.length() > maxCheckpointBytes) {
            logger
                .warn(new ParameterizedMessage("[{}]'s model empty or too large: [{}] bytes", modelId, model == null ? 0 : model.length()));
            return null;
        }
        return toRCFCaster(model);
    }

    /**
     * Load json checkpoint into models. Used in HC forecasting.
     *
     * @param checkpoint json checkpoint contents
     * @param modelId Model Id
     * @return a pair of entity model and its last checkpoint time; or empty if
     *  the raw checkpoint is too large
     */
    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "will change to org.opensearch.secure_sm.AccessController and thus no need of it since 3.5")
    public ModelState<RCFCaster> fromEntityModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId,
        String tenantId
    ) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<ModelState<RCFCaster>>) () -> {

                RCFCaster rcfCaster = loadRCFCaster(checkpoint, modelId);

                Entity entity = null;
                Object serializedEntity = checkpoint.get(CommonName.ENTITY_KEY);
                if (serializedEntity != null) {
                    try {
                        entity = Entity.fromJsonArray(serializedEntity);
                    } catch (Exception e) {
                        logger.error(new ParameterizedMessage("fail to parse entity", serializedEntity), e);
                    }
                }

                ModelState<RCFCaster> modelState = new ModelState<RCFCaster>(
                    rcfCaster,
                    modelId,
                    configId,
                    tenantId,
                    ModelManager.ModelType.RCFCASTER.getName(),
                    clock,
                    0,
                    Optional.ofNullable(entity),
                    loadSampleQueue(checkpoint, modelId)
                );

                modelState.setLastCheckpointTime(loadTimestamp(checkpoint, modelId));

                return modelState;
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint " + modelId, e);
            // checkpoint corrupted (e.g., a checkpoint not recognized by current code
            // due to bugs). Better redo training.
            return null;
        }
    }

    public Optional<String> toCheckpoint(Optional<RCFCaster> caster) {
        if (caster.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> checkpoint = Optional.empty();
        Map.Entry<LinkedBuffer, Boolean> result = checkoutOrNewBuffer();
        LinkedBuffer buffer = result.getKey();
        boolean needCheckin = result.getValue();
        try {
            checkpoint = toCheckpoint(caster, buffer);
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
                    checkpoint = toCheckpoint(caster, LinkedBuffer.allocate(serializeRCFBufferSize));
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
    private Optional<String> toCheckpoint(Optional<RCFCaster> caster, LinkedBuffer buffer) {
        if (caster.isEmpty()) {
            return Optional.empty();
        }
        try {
            byte[] bytes = AccessController.doPrivileged((PrivilegedAction<byte[]>) () -> {
                RCFCasterState casterState = mapper.toState(caster.get());
                return ProtostuffIOUtil.toByteArray(casterState, rcfCasterSchema, buffer);
            });
            return Optional.ofNullable(Base64.getEncoder().encodeToString(bytes));
        } finally {
            buffer.clear();
        }
    }

    /**
     * Prepare for index request using the contents of the given model state. Used in HC forecasting.
     * @param modelState an entity model state
     * @return serialized JSON map or empty map if the state is too bloated
     * @throws IOException  when serialization fails
     */
    @Override
    public Map<String, Object> toIndexSource(ModelState<RCFCaster> modelState) throws IOException {
        Map<String, Object> source = new HashMap<>();
        Optional<RCFCaster> model = modelState.getModel();

        Optional<String> serializedModel = toCheckpoint(model);
        if (serializedModel.isPresent() && serializedModel.get().length() <= maxCheckpointBytes) {
            // we cannot pass Optional as OpenSearch does not know how to serialize an Optional value
            source.put(CommonName.FIELD_MODEL, serializedModel.get());
        } else {
            logger
                .warn(
                    new ParameterizedMessage(
                        "[{}]'s model is empty or too large: [{}] bytes",
                        modelState.getModelId(),
                        serializedModel.isPresent() ? serializedModel.get().length() : 0
                    )
                );
        }
        Optional<Sample[]> samples = toCheckpoint(modelState.getSamples());
        if (samples.isPresent()) {
            source.put(CommonName.SAMPLE_QUEUE, samples.get());
        }
        // if there are no samples and no model, no need to index as other information are meta data
        if (!source.containsKey(CommonName.SAMPLE_QUEUE) && !source.containsKey(CommonName.FIELD_MODEL)) {
            logger.info("nothing to save for [{}]", modelState.getModelId());
            return source;
        }

        source.put(ForecastCommonName.FORECASTER_ID_KEY, modelState.getConfigId());
        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        source.put(CommonName.SCHEMA_VERSION_FIELD, dataManagement.getSchemaVersion(ForecastIndex.CHECKPOINT));

        Optional<Entity> entity = modelState.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }
        return source;
    }

}
