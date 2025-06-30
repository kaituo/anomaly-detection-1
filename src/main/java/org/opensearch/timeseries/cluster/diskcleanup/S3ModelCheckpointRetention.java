/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster.diskcleanup;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.Strings;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Retention job for S3-backed checkpoints.
 * S3-backed checkpoints are cleaned up by TTL only since they are not constrained
 * by index shard sizing.
 */
public class S3ModelCheckpointRetention implements Runnable {

    private static final Logger LOG = LogManager.getLogger(S3ModelCheckpointRetention.class);
    private static final int MAX_OBJECTS_PER_DELETE = 1000;

    private final Duration defaultCheckpointTtl;
    private final Clock clock;
    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final String checkpointPrefix;

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    public S3ModelCheckpointRetention(
        Duration defaultCheckpointTtl,
        Clock clock,
        String bucketName,
        String region,
        String checkpointPrefix
    ) {
        this(
            defaultCheckpointTtl,
            clock,
            AccessController
                .doPrivileged(
                    (PrivilegedAction<S3AsyncClient>) () -> S3AsyncClient
                        .builder()
                        .region(Region.of(region))
                        .credentialsProvider(createCredentialsProvider())
                        .build()
                ),
            bucketName,
            checkpointPrefix
        );
    }

    public S3ModelCheckpointRetention(
        Duration defaultCheckpointTtl,
        Clock clock,
        S3AsyncClient s3Client,
        String bucketName,
        String checkpointPrefix
    ) {
        this.defaultCheckpointTtl = defaultCheckpointTtl;
        this.clock = clock;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.checkpointPrefix = checkpointPrefix;
    }

    private static AwsCredentialsProvider createCredentialsProvider() {
        return org.opensearch.timeseries.util.SecurityUtil.createCredentialsProvider();
    }

    @Override
    public void run() {
        try {
            deleteOlderThan(defaultCheckpointTtl);
        } catch (Exception e) {
            LOG.error("Failed to run S3 checkpoint retention", e);
        }
    }

    private void deleteOlderThan(Duration ttl) {
        List<ObjectIdentifier> expired = listCheckpoints()
            .stream()
            .filter(obj -> isOlderThan(obj, ttl))
            .map(this::toIdentifier)
            .collect(Collectors.toList());

        if (!expired.isEmpty()) {
            deleteObjects(expired);
        }
    }

    private boolean isOlderThan(S3Object object, Duration ttl) {
        Instant cutoff = clock.instant().minus(ttl);
        return object.lastModified() != null && object.lastModified().isBefore(cutoff);
    }

    private ObjectIdentifier toIdentifier(S3Object object) {
        return ObjectIdentifier.builder().key(object.key()).build();
    }

    private List<S3Object> listCheckpoints() {
        if (Strings.isNullOrEmpty(bucketName)) {
            return Collections.emptyList();
        }

        List<S3Object> results = new ArrayList<>();
        String continuationToken = null;

        while (true) {
            try {
                ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucketName).prefix(checkpointPrefix);
                if (!Strings.isNullOrEmpty(continuationToken)) {
                    builder.continuationToken(continuationToken);
                }
                ListObjectsV2Response response = s3Client.listObjectsV2(builder.build()).join();
                results.addAll(response.contents());
                if (!response.isTruncated() || Strings.isNullOrEmpty(response.nextContinuationToken())) {
                    break;
                }
                continuationToken = response.nextContinuationToken();
            } catch (SdkException e) {
                LOG.error("Failed to list S3 checkpoints under prefix {}", checkpointPrefix, e);
                return Collections.emptyList();
            } catch (RuntimeException e) {
                LOG.error("Failed to list S3 checkpoints under prefix {}", checkpointPrefix, e);
                return Collections.emptyList();
            }
        }
        return results;
    }

    private void deleteObjects(List<ObjectIdentifier> objects) {
        for (int i = 0; i < objects.size(); i += MAX_OBJECTS_PER_DELETE) {
            List<ObjectIdentifier> batch = objects.subList(i, Math.min(objects.size(), i + MAX_OBJECTS_PER_DELETE));
            try {
                DeleteObjectsResponse response = s3Client
                    .deleteObjects(
                        DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(batch).build()).build()
                    )
                    .join();
                int deletedCount = response.deleted() == null ? 0 : response.deleted().size();
                LOG.info("Deleted {} S3 checkpoints for prefix {} (batch size {})", deletedCount, checkpointPrefix, batch.size());
            } catch (SdkException e) {
                LOG.error("Failed to delete S3 checkpoints for prefix {}", checkpointPrefix, e);
            } catch (RuntimeException e) {
                LOG.error("Failed to delete S3 checkpoints for prefix {}", checkpointPrefix, e);
            }
        }
    }
}
