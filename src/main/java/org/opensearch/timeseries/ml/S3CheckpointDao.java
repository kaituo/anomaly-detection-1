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

package org.opensearch.timeseries.ml;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.get.MultiGetResponse.Failure;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.timeseries.cluster.diskcleanup.S3ModelCheckpointRetention;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * An S3-based implementation of CheckpointDaoInterface.
 * It uses Amazon S3 to store and retrieve ML model checkpoints instead of OpenSearch indices.
 *
 * @param <RCFModelType> The type of RCF model.
 */
public abstract class S3CheckpointDao<RCFModelType> implements CheckpointDaoInterface<RCFModelType> {

    private static final Logger logger = LogManager.getLogger(S3CheckpointDao.class);
    private static final String ENTITY_MODEL_ID_INFIX = "_entity_";
    private static final int ENTITY_KEY_SEGMENT_LENGTH = 2;
    private static final int ENTITY_KEY_SEGMENT_DEPTH = 3;

    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String region;
    private final CheckpointCodec<RCFModelType> checkpointCodec;

    /**
     * Constructs a new S3CheckpointDao instance.
     *
     * @param bucketName The name of the S3 bucket.
     * @param region The AWS region for the S3 bucket.
     * @param checkpointCodec The codec used for checkpoint serialization/deserialization.
     */
    public S3CheckpointDao(String bucketName, String region, CheckpointCodec<RCFModelType> checkpointCodec) {
        this(bucketName, region, checkpointCodec, createS3Client(region));
    }

    S3CheckpointDao(String bucketName, String region, CheckpointCodec<RCFModelType> checkpointCodec, S3AsyncClient s3Client) {
        this.bucketName = Objects.requireNonNull(bucketName, "bucketName must not be null");
        this.region = Objects.requireNonNull(region, "region must not be null");
        this.checkpointCodec = checkpointCodec;
        this.s3Client = Objects.requireNonNull(s3Client, "s3Client must not be null");
        logger
            .info(
                "Initialized S3 checkpoint store [{}] for bucket [{}] in region [{}] with codec [{}]",
                getClass().getName(),
                this.bucketName,
                this.region,
                checkpointCodec.getClass().getName()
            );
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    private static S3AsyncClient createS3Client(String region) {
        return AccessController
            .doPrivileged(
                (PrivilegedAction<S3AsyncClient>) () -> S3AsyncClient
                    .builder()
                    .region(Region.of(region))
                    .credentialsProvider(SecurityUtil.createCredentialsProvider())
                    .build()
            );
    }

    @Override
    public String resolveCheckpointIndexName(String tenantId, String configId, String modelId, String defaultIndexName) {
        if (Strings.isEmpty(tenantId) || Strings.isEmpty(configId)) {
            logger
                .warn(
                    "Falling back to default checkpoint identifier [{}] because tenantId [{}] or configId [{}] is empty for model [{}]",
                    defaultIndexName,
                    tenantId,
                    configId,
                    modelId
                );
            return defaultIndexName;
        }

        String basePrefix = StringUtil.sanitizeId(tenantId) + "/" + configId;
        String entityId = extractEntityIdentifier(modelId);
        if (Strings.isEmpty(entityId)) {
            logger
                .debug(
                    "Resolved S3 checkpoint prefix [{}] for config [{}], model [{}], tenant [{}]",
                    basePrefix,
                    configId,
                    modelId,
                    tenantId
                );
            return basePrefix;
        }
        String keyPrefix = buildEntityModelKey(basePrefix, entityId);
        logger.debug("Resolved S3 checkpoint prefix [{}] for config [{}], model [{}], tenant [{}]", keyPrefix, configId, modelId, tenantId);
        return keyPrefix;
    }

    @Override
    public void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener) {
        String keyPrefix = resolveCheckpointIndexName(config.getTenantId(), config.getId(), modelId, "");

        s3Client.deleteObject(r -> r.bucket(bucketName).key(getS3Key(keyPrefix, modelId))).whenComplete((response, error) -> {
            if (error != null) {
                logger.error("Failed to delete model checkpoint from S3 for modelId: " + modelId, error);
                listener.onFailure(new TimeSeriesException("Failed to delete model checkpoint from S3", error));
            } else {
                logger.info("Successfully deleted model checkpoint from S3 for modelId: {}", modelId);
                listener.onResponse(null);
            }
        });
    }

    @Override
    public Runnable createRetentionTask(Duration checkpointTtl, Clock clock) {
        Objects.requireNonNull(checkpointTtl, "checkpointTtl must not be null");
        Objects.requireNonNull(clock, "clock must not be null");
        return new S3ModelCheckpointRetention(checkpointTtl, clock, s3Client, bucketName, getRetentionPrefix());
    }

    @Override
    public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
        List<CompletableFuture<BulkItemResponse>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        logger
            .info(
                "Starting S3 checkpoint batch write with [{}] request(s) to bucket [{}] in region [{}]",
                request.requests().size(),
                bucketName,
                region
            );

        request.requests().forEach(docWriteRequest -> {
            if (docWriteRequest instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
                String modelId = updateRequest.id();
                try {
                    BytesReference source = updateRequest.doc().source();
                    byte[] bytes = BytesReference.toBytes(source);
                    String s3Key = getS3Key(updateRequest.index(), modelId);
                    logger
                        .info(
                            "Attempting S3 checkpoint write for model [{}] to bucket [{}], key [{}], payloadBytes [{}]",
                            modelId,
                            bucketName,
                            s3Key,
                            bytes.length
                        );

                    // The index name is used as the key prefix for S3 keys.
                    ShardId shardId = new ShardId(updateRequest.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0);
                    UpdateResponse.Builder responseBuilder = new UpdateResponse.Builder();
                    responseBuilder.setShardId(shardId);
                    responseBuilder.setId(modelId);
                    responseBuilder.setVersion(1L);
                    responseBuilder.setSeqNo(0L);
                    responseBuilder.setPrimaryTerm(1L);
                    responseBuilder.setResult(DocWriteResponse.Result.UPDATED);
                    responseBuilder.setShardInfo(new ShardInfo(1, 1));
                    UpdateResponse fakeResponse = responseBuilder.build();
                    PutObjectRequest putRequest = PutObjectRequest.builder().bucket(bucketName).key(s3Key).build();
                    futures.add(s3Client.putObject(putRequest, AsyncRequestBody.fromBytes(bytes)).thenApply(response -> {
                        logger
                            .info(
                                "S3 checkpoint write succeeded for model [{}] to bucket [{}], key [{}], eTag [{}]",
                                modelId,
                                bucketName,
                                s3Key,
                                response.eTag()
                            );
                        return new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE, fakeResponse);
                    }).exceptionally(ex -> {
                        logger.error("S3 checkpoint write failed for model [{}] to bucket [{}], key [{}]", modelId, bucketName, s3Key, ex);
                        return new BulkItemResponse(
                            0,
                            org.opensearch.action.DocWriteRequest.OpType.UPDATE,
                            new BulkItemResponse.Failure(
                                updateRequest.index(),
                                modelId,
                                new TimeSeriesException("Failed to write to S3", ex)
                            )
                        );
                    }));
                } catch (Exception e) {
                    logger
                        .error(
                            "Failed to prepare S3 checkpoint write for model [{}] in bucket [{}] with target prefix [{}]",
                            modelId,
                            bucketName,
                            updateRequest.index(),
                            e
                        );
                    futures
                        .add(
                            CompletableFuture
                                .completedFuture(
                                    new BulkItemResponse(
                                        0,
                                        org.opensearch.action.DocWriteRequest.OpType.UPDATE,
                                        new BulkItemResponse.Failure(
                                            updateRequest.index(),
                                            modelId,
                                            new TimeSeriesException("Failed to prepare S3 write", e)
                                        )
                                    )
                                )
                        );
                }
            } else {
                logger
                    .warn(
                        "Unsupported request type [{}] in S3 checkpoint batch write for bucket [{}]",
                        docWriteRequest.getClass().getName(),
                        bucketName
                    );
            }
        });

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenAccept(v -> {
            List<BulkItemResponse> responses = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
            long tookInMillis = System.currentTimeMillis() - startTime;
            long failures = responses.stream().filter(BulkItemResponse::isFailed).count();
            logger
                .info(
                    "Completed S3 checkpoint batch write with [{}] item(s), [{}] failure(s), took [{}] ms, bucket [{}]",
                    responses.size(),
                    failures,
                    tookInMillis,
                    bucketName
                );
            listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[0]), tookInMillis));
        }).exceptionally(e -> {
            logger.error("Error during S3 checkpoint batch write for bucket [{}]", bucketName, e);
            listener.onFailure(new TimeSeriesException("Error during S3 batch write", e));
            return null;
        });
    }

    private String getS3Key(String keyPrefix, String objectName) {
        return keyPrefix + "/" + objectName;
    }

    /**
     * Root prefix for store-wide retention scans. The default S3-backed implementation uses the
     * whole bucket; custom stores can override this to scope retention to a sub-prefix.
     */
    protected String getRetentionPrefix() {
        return "";
    }

    @Override
    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        List<CompletableFuture<MultiGetItemResponse>> futures = new ArrayList<>();
        request.getItems().forEach(item -> {
            String modelId = item.id();
            GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(getS3Key(item.index(), modelId)).build();
            futures.add(s3Client.getObject(getRequest, AsyncResponseTransformer.toBytes()).thenApply(responseBytes -> {
                BytesReference content = BytesReference.fromByteBuffer(responseBytes.asByteBuffer());
                // sequence number >= 0 while primaryTerm >= 1 indicates a successful read
                GetResult getResult = new GetResult(item.index(), modelId, 0, 1, 0, true, content, null, null);
                return new MultiGetItemResponse(new GetResponse(getResult), null);
            }).exceptionally(ex -> {
                if (ex.getCause() instanceof NoSuchKeyException) {
                    // unsigned sequence number and primary term indicate not exists
                    GetResult getResult = new GetResult(
                        item.index(),
                        modelId,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        0,
                        false,
                        null,
                        null,
                        null
                    );
                    return new MultiGetItemResponse(new GetResponse(getResult), null);
                }
                logger.error("Error during S3 batch read for modelId: " + modelId, ex);
                return new MultiGetItemResponse(null, new Failure(item.index(), modelId, (Exception) ex));
            }));
        });

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenAccept(v -> {
            List<MultiGetItemResponse> responses = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
            listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[0])));
        }).exceptionally(e -> {
            listener.onFailure(new TimeSeriesException("Error during S3 batch read", e));
            return null;
        });
    }

    @Override
    public void deleteModelCheckpointByConfigId(String tenantId, String configId) {
        String prefix = resolveCheckpointIndexName(tenantId, configId, "", "");
        deleteModelCheckpointByConfigIdPage(configId, prefix, null, 0L);
    }

    private void deleteModelCheckpointByConfigIdPage(String configId, String prefix, String continuationToken, long deletedCheckpoints) {
        ListObjectsV2Request.Builder listRequest = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix);
        if (continuationToken != null) {
            listRequest.continuationToken(continuationToken);
        }

        s3Client.listObjectsV2(listRequest.build()).whenComplete((listResponse, listError) -> {
            if (listError != null) {
                logger.error("Error listing S3 objects to delete for configId: " + configId, listError);
                return;
            }

            List<ObjectIdentifier> toDelete = listResponse
                .contents()
                .stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .collect(Collectors.toList());

            if (toDelete.isEmpty()) {
                if (listResponse.isTruncated()) {
                    deleteModelCheckpointByConfigIdPage(configId, prefix, listResponse.nextContinuationToken(), deletedCheckpoints);
                } else {
                    logger.info("Successfully deleted {} model checkpoints from S3 for configId: {}", deletedCheckpoints, configId);
                }
                return;
            }

            DeleteObjectsRequest deleteRequest = DeleteObjectsRequest
                .builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(toDelete).build())
                .build();

            s3Client.deleteObjects(deleteRequest).whenComplete((deleteResponse, deleteError) -> {
                if (deleteError != null) {
                    logger.error("Failed to delete model checkpoints from S3 for configId: " + configId, deleteError);
                    return;
                }
                if (!deleteResponse.errors().isEmpty()) {
                    logger.warn("S3 reported {} checkpoint delete errors for configId: {}", deleteResponse.errors().size(), configId);
                }

                long totalDeleted = deletedCheckpoints + deleteResponse.deleted().size();
                if (listResponse.isTruncated()) {
                    deleteModelCheckpointByConfigIdPage(configId, prefix, null, totalDeleted);
                } else {
                    logger.info("Successfully deleted {} model checkpoints from S3 for configId: {}", totalDeleted, configId);
                }
            });
        });
    }

    @Override
    public CheckpointCodec<RCFModelType> getCodec() {
        return checkpointCodec;
    }

    static String buildEntityModelKey(String basePrefix, String entityId) {
        StringBuilder keyBuilder = new StringBuilder(basePrefix);

        int index = 0;
        int segmentCount = 0;
        while (segmentCount < ENTITY_KEY_SEGMENT_DEPTH && (index + ENTITY_KEY_SEGMENT_LENGTH) < entityId.length()) {
            keyBuilder.append("/").append(entityId, index, index + ENTITY_KEY_SEGMENT_LENGTH);
            index += ENTITY_KEY_SEGMENT_LENGTH;
            segmentCount++;
        }

        if (index < entityId.length()) {
            keyBuilder.append("/").append(entityId.substring(index));
        }

        return keyBuilder.toString();
    }

    static String extractEntityIdentifier(String modelId) {
        if (Strings.isEmpty(modelId)) {
            return "";
        }
        int entityInfixIndex = modelId.indexOf(ENTITY_MODEL_ID_INFIX);
        if (entityInfixIndex < 0) {
            return "";
        }
        int entityStartIndex = entityInfixIndex + ENTITY_MODEL_ID_INFIX.length();
        return entityStartIndex < modelId.length() ? modelId.substring(entityStartIndex) : "";
    }
}
