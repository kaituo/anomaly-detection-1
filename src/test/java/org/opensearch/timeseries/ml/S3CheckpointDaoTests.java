/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import static org.mockito.Mockito.mock;

import org.opensearch.test.OpenSearchTestCase;

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

    @SuppressWarnings("unchecked")
    private S3CheckpointDao<Object> createDao() {
        return new S3CheckpointDao<Object>("bucket", "us-east-1", mock(CheckpointCodec.class)) {
        };
    }
}
