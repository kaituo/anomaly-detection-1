/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ratelimit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;

/** Unit tests for the FeatureRequest constructor’s model-id branch logic. */
public class FeatureRequestTests extends OpenSearchTestCase {

    private static final String CONFIG_ID = "cfg-A";
    private static final double[] FEATURES = new double[] { 1.0, 2.0 };
    private static final long DATA_START = Instant.now().toEpochMilli();

    /* ------------------------------------------------------------------
       entity.getModelId() returns empty  → modelId == null
       ------------------------------------------------------------------ */
    public void testConstructor_setsModelIdNullWhenEmptyOptional() {
        Entity mockEntity = mock(Entity.class);
        when(mockEntity.getModelId(null, CONFIG_ID)).thenReturn(Optional.empty());

        FeatureRequest req = new FeatureRequest(/*expirationEpochMs*/ Instant.now().plusSeconds(60).toEpochMilli(),
            CONFIG_ID,
            RequestPriority.LOW,
            FEATURES,
            DATA_START,
            mockEntity,
            /*taskId*/ null,
            null,
            null,
            System.currentTimeMillis()
        );

        assertNull("Expected modelId to be null when getModelId() is empty", req.getModelId());
        assertTrue("Entity should be present", req.getEntity().isPresent());
        verify(mockEntity, times(1)).getModelId(null, CONFIG_ID); // called once inside ternary
    }

    /* ------------------------------------------------------------------
       entity.getModelId() returns non-empty  → modelId == value
       ------------------------------------------------------------------ */
    public void testConstructor_setsModelIdWhenOptionalPresent() {
        String expectedModelId = "model-123";
        Entity mockEntity = mock(Entity.class);
        when(mockEntity.getModelId(null, CONFIG_ID)).thenReturn(Optional.of(expectedModelId));

        FeatureRequest req = new FeatureRequest(/*expirationEpochMs*/ Instant.now().plusSeconds(60).toEpochMilli(),
            CONFIG_ID,
            RequestPriority.MEDIUM,
            FEATURES,
            DATA_START,
            mockEntity,
            /*taskId*/ "task-X",
            null,
            null,
            System.currentTimeMillis()
        );

        assertEquals("modelId mismatch", expectedModelId, req.getModelId());
        assertTrue("Entity should be present", req.getEntity().isPresent());
        verify(mockEntity, times(2)).getModelId(null, CONFIG_ID); // both branches call it twice
    }

    public void testSingleStreamConstructor_retainsInlineConfig() {
        Config config = mock(Config.class);
        when(config.getDataSourceId()).thenReturn("data-source-1");

        FeatureRequest req = new FeatureRequest(
            Instant.now().plusSeconds(60).toEpochMilli(),
            CONFIG_ID,
            RequestPriority.MEDIUM,
            "model-123",
            FEATURES,
            DATA_START,
            "task-X",
            "tenant-1",
            config,
            System.currentTimeMillis()
        );

        assertEquals("model-123", req.getModelId());
        assertTrue(req.getConfig().isPresent());
        assertSame(config, req.getConfig().get());
        assertEquals("data-source-1", req.getDataSourceId());
    }

    public void testConstructorRetainsRequestCreationTime() {
        Entity mockEntity = mock(Entity.class);
        when(mockEntity.getModelId("tenant-1", CONFIG_ID)).thenReturn(Optional.of("model-123"));
        long requestTimeMillis = DATA_START - 10_000L;
        String dataSourceId = "data-source-1";

        FeatureRequest req = new FeatureRequest(
            Instant.now().plusSeconds(60).toEpochMilli(),
            CONFIG_ID,
            RequestPriority.LOW,
            FEATURES,
            DATA_START,
            mockEntity,
            null,
            "tenant-1",
            dataSourceId,
            requestTimeMillis
        );

        assertEquals(requestTimeMillis, req.getRequestTimeMillis());
        assertEquals(DATA_START, req.getDataStartTimeMillis());
        assertEquals(dataSourceId, req.getDataSourceId());
    }
}
