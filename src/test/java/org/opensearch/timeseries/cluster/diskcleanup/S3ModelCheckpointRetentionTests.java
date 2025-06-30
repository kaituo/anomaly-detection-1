/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster.diskcleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.timeseries.AbstractTimeSeriesTest;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3ModelCheckpointRetentionTests extends AbstractTimeSeriesTest {

    private static final Duration DEFAULT_CHECKPOINT_TTL = Duration.ofDays(3);
    private static final long GIB = 1024L * 1024L * 1024L;

    @Mock
    private S3AsyncClient s3Client;

    private final Clock clock = Clock.fixed(Instant.parse("2026-04-08T00:00:00Z"), ZoneOffset.UTC);

    @Test
    public void testRunDeletesExpiredCheckpointsByTtl() {
        ListObjectsV2Response listResponse = ListObjectsV2Response
            .builder()
            .contents(
                checkpointObject("expired-checkpoint", clock.instant().minus(Duration.ofDays(4)), 5L),
                checkpointObject("recent-checkpoint", clock.instant().minus(Duration.ofDays(1)), 7L)
            )
            .isTruncated(false)
            .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.completedFuture(listResponse));
        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build()));

        new S3ModelCheckpointRetention(DEFAULT_CHECKPOINT_TTL, clock, s3Client, "bucket", "checkpoints/").run();

        ArgumentCaptor<DeleteObjectsRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3Client).deleteObjects(deleteRequestCaptor.capture());
        List<ObjectIdentifier> deletedObjects = deleteRequestCaptor.getValue().delete().objects();
        assertEquals(1, deletedObjects.size());
        assertEquals("expired-checkpoint", deletedObjects.get(0).key());
    }

    @Test
    public void testRunDoesNotDeleteBasedOnSize() {
        ListObjectsV2Response listResponse = ListObjectsV2Response
            .builder()
            .contents(
                checkpointObject("older-but-within-ttl", clock.instant().minus(Duration.ofDays(2)).minusSeconds(1), 40L * GIB),
                checkpointObject("recent-checkpoint", clock.instant().minus(Duration.ofHours(12)), 20L * GIB)
            )
            .isTruncated(false)
            .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.completedFuture(listResponse));

        new S3ModelCheckpointRetention(DEFAULT_CHECKPOINT_TTL, clock, s3Client, "bucket", "checkpoints/").run();

        verify(s3Client, never()).deleteObjects(any(DeleteObjectsRequest.class));
    }

    private S3Object checkpointObject(String key, Instant lastModified, long size) {
        return S3Object.builder().key(key).lastModified(lastModified).size(size).build();
    }
}
