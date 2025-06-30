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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.get.MultiGetResponse.Failure;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.SecurityUtil;

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

    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final CheckpointCodec<RCFModelType> checkpointCodec;

    /**
     * Constructs a new S3CheckpointDao instance.
     *
     * @param bucketName The name of the S3 bucket.
     * @param region The AWS region for the S3 bucket.
     * @param checkpointCodec The codec used for checkpoint serialization/deserialization.
     */
    public S3CheckpointDao(String bucketName, String region, CheckpointCodec<RCFModelType> checkpointCodec) {
        this.s3Client =  AccessController
        .doPrivileged(
            (PrivilegedAction<S3AsyncClient>) () -> S3AsyncClient
                .builder()
                .region(Region.of(region))
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        );;
        this.bucketName = Objects.requireNonNull(bucketName, "bucketName must not be null");
        this.checkpointCodec = checkpointCodec;
    }

    @Override
    public void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener) {
        String keyPrefix = IndexUtils.resolveIndexName(config.getTenantId(), config.getId(), modelId, "");

        s3Client
            .deleteObject(r -> r.bucket(bucketName).key(getS3Key(keyPrefix, modelId)))
            .whenComplete((response, error) -> {
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
    public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
        List<CompletableFuture<BulkItemResponse>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        request.requests().forEach(docWriteRequest -> {
            if (docWriteRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                String modelId = indexRequest.id();
                try {
                    BytesReference source = indexRequest.source();
                    byte[] bytes = BytesReference.toBytes(source);

                    // The index name is used as the key prefix for S3 keys.
                    PutObjectRequest putRequest = PutObjectRequest.builder().bucket(bucketName).key(getS3Key(indexRequest.index(), modelId)).build();
                    futures
                        .add(
                            s3Client
                                .putObject(putRequest, AsyncRequestBody.fromBytes(bytes))
                                .thenApply(
                                    response -> new BulkItemResponse(
                                        0,
                                        org.opensearch.action.DocWriteRequest.OpType.UPDATE,
                                        new UpdateResponse.Builder().build()
                                    )
                                )
                                .exceptionally(
                                    ex -> new BulkItemResponse(
                                        0,
                                        org.opensearch.action.DocWriteRequest.OpType.UPDATE,
                                        new BulkItemResponse.Failure(
                                            indexRequest.index(),
                                            modelId,
                                            new TimeSeriesException("Failed to write to S3", ex)
                                        )
                                    )
                                )
                        );
                } catch (Exception e) {
                    futures
                        .add(
                            CompletableFuture
                                .completedFuture(
                                    new BulkItemResponse(
                                        0,
                                        org.opensearch.action.DocWriteRequest.OpType.UPDATE,
                                        new BulkItemResponse.Failure(
                                            indexRequest.index(),
                                            modelId,
                                            new TimeSeriesException("Failed to prepare S3 write", e)
                                        )
                                    )
                                )
                        );
                }
            }
        });

        CompletableFuture
            .allOf(futures.toArray(new CompletableFuture[0]))
            .thenAccept(v -> {
                List<BulkItemResponse> responses = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
                listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[0]), System.currentTimeMillis() - startTime));
            })
            .exceptionally(e -> {
                listener.onFailure(new TimeSeriesException("Error during S3 batch write", e));
                return null;
            });
    }

    private String getS3Key(String keyPrefix, String objectName) {
        return keyPrefix + "/" + objectName;
    }

    @Override
    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        List<CompletableFuture<MultiGetItemResponse>> futures = new ArrayList<>();
        request.getItems().forEach(item -> {
            String modelId = item.id();
            GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(getS3Key(item.index(), modelId)).build();
            futures
                .add(
                    s3Client
                        .getObject(getRequest, AsyncResponseTransformer.toBytes())
                        .thenApply(responseBytes -> {
                            BytesReference content = BytesReference.fromByteBuffer(responseBytes.asByteBuffer());
                            GetResult getResult = new GetResult(item.index(), modelId, 0, 0, 0, true, content, null, null);
                            return new MultiGetItemResponse(new GetResponse(getResult), null);
                        })
                        .exceptionally(ex -> {
                            if (ex.getCause() instanceof NoSuchKeyException) {
                                GetResult getResult = new GetResult(item.index(), modelId, 0, 0, 0, false, null, null, null);
                                return new MultiGetItemResponse(new GetResponse(getResult), null);
                            }
                            return new MultiGetItemResponse(null, new Failure(item.index(), modelId, (Exception) ex));
                        })
                );
        });

        CompletableFuture
            .allOf(futures.toArray(new CompletableFuture[0]))
            .thenAccept(v -> {
                List<MultiGetItemResponse> responses = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
                listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[0])));
            })
            .exceptionally(e -> {
                listener.onFailure(new TimeSeriesException("Error during S3 batch read", e));
                return null;
            });
    }

    @Override
    public void deleteModelCheckpointByConfigId(String tenantId, String configId) {
        String prefix = IndexUtils.resolveIndexName(tenantId, configId, "", "");
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();

        s3Client.listObjectsV2(listRequest).thenAccept(listResponse -> {
            if (!listResponse.contents().isEmpty()) {
                List<ObjectIdentifier> toDelete = listResponse
                    .contents()
                    .stream()
                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                    .collect(Collectors.toList());

                DeleteObjectsRequest deleteRequest = DeleteObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(toDelete).build())
                    .build();

                s3Client
                    .deleteObjects(deleteRequest)
                    .whenComplete((deleteResponse, error) -> {
                        if (error != null) {
                            logger.error("Failed to delete model checkpoints from S3 for configId: " + configId, error);
                        } else {
                            logger.info("Successfully deleted model checkpoints from S3 for configId: {}", configId);
                        }
                    });
            }
        }).exceptionally(e -> {
            logger.error("Error listing S3 objects to delete for configId: " + configId, e);
            return null;
        });
    }

    @Override
    public CheckpointCodec<RCFModelType> getCodec() {
        return checkpointCodec;
    }
}
