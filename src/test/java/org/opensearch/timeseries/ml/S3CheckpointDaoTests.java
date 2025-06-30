/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.mockito.ArgumentCaptor;
import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3CheckpointDaoTests extends OpenSearchTestCase {

    public void testResolveCheckpointIndexNameForSingleStreamModel() {
        S3CheckpointDao<Object> dao = createDao();

        String prefix = dao.resolveCheckpointIndexName("application:data-source", "config-1", "model-1", "default-index");

        assertEquals("application_data-source/config-1", prefix);
    }

    public void testResolveCheckpointIndexNameForEntityModel() {
        S3CheckpointDao<Object> dao = createDao();

        String prefix = dao.resolveCheckpointIndexName("application:data-source", "config-1", "detector_entity_abcdefghi", "default-index");

        assertEquals("application_data-source/config-1/ab/cd/ef/ghi", prefix);
    }

    public void testResolveCheckpointIndexNameForDeleteByConfigPrefix() {
        S3CheckpointDao<Object> dao = createDao();

        String prefix = dao.resolveCheckpointIndexName("application:data-source", "config-1", "", "default-index");

        assertEquals("application_data-source/config-1", prefix);
    }

    public void testDeleteModelCheckpointByConfigIdRelistsPrefixAfterDeletingTruncatedPage() {
        S3AsyncClient s3Client = mock(S3AsyncClient.class);
        S3CheckpointDao<Object> dao = createDao(s3Client);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(
                CompletableFuture
                    .completedFuture(
                        ListObjectsV2Response
                            .builder()
                            .contents(S3Object.builder().key("tenant/config/model-1").build())
                            .isTruncated(true)
                            .nextContinuationToken("token-after-deleted-page")
                            .build()
                    )
            )
            .thenReturn(
                CompletableFuture
                    .completedFuture(
                        ListObjectsV2Response
                            .builder()
                            .contents(S3Object.builder().key("tenant/config/model-2").build())
                            .isTruncated(false)
                            .build()
                    )
            );
        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class)))
            .thenReturn(
                CompletableFuture
                    .completedFuture(
                        DeleteObjectsResponse.builder().deleted(DeletedObject.builder().key("tenant/config/model-1").build()).build()
                    )
            )
            .thenReturn(
                CompletableFuture
                    .completedFuture(
                        DeleteObjectsResponse.builder().deleted(DeletedObject.builder().key("tenant/config/model-2").build()).build()
                    )
            );

        dao.deleteModelCheckpointByConfigId("tenant", "config");

        ArgumentCaptor<ListObjectsV2Request> listRequestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3Client, times(2)).listObjectsV2(listRequestCaptor.capture());
        List<ListObjectsV2Request> listRequests = listRequestCaptor.getAllValues();
        assertEquals("tenant/config", listRequests.get(0).prefix());
        assertNull(listRequests.get(0).continuationToken());
        assertEquals("tenant/config", listRequests.get(1).prefix());
        assertNull(listRequests.get(1).continuationToken());

        ArgumentCaptor<DeleteObjectsRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3Client, times(2)).deleteObjects(deleteRequestCaptor.capture());
        List<DeleteObjectsRequest> deleteRequests = deleteRequestCaptor.getAllValues();
        assertEquals("tenant/config/model-1", deleteRequests.get(0).delete().objects().get(0).key());
        assertEquals("tenant/config/model-2", deleteRequests.get(1).delete().objects().get(0).key());
    }

    @SuppressWarnings("unchecked")
    private S3CheckpointDao<Object> createDao() {
        return new S3CheckpointDao<Object>("bucket", "us-east-1", mock(CheckpointCodec.class)) {
        };
    }

    @SuppressWarnings("unchecked")
    private S3CheckpointDao<Object> createDao(S3AsyncClient s3Client) {
        return new S3CheckpointDao<Object>("bucket", "us-east-1", mock(CheckpointCodec.class), s3Client) {
        };
    }
}
