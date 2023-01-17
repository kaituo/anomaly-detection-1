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

package org.opensearch.ad.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.ml.ADEntityColdStart;
import org.opensearch.ad.ml.ADModelState;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.HCCacheProvider;
import org.opensearch.timeseries.ml.createFromValueOnlySamples;
import org.opensearch.timeseries.ratelimit.EntityRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import test.org.opensearch.ad.util.MLUtil;

public class EntityColdStartWorkerTests extends AbstractRateLimitingTest {
    ClusterService clusterService;
    ADColdStartWorker worker;
    ADEntityColdStart entityColdStarter;
    HCCacheProvider cacheProvider;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_CONCURRENCY
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        entityColdStarter = mock(ADEntityColdStart.class);

        cacheProvider = mock(HCCacheProvider.class);

        // Integer.MAX_VALUE makes a huge heap
        worker = new ADColdStartWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(CircuitBreakerService.class),
            threadPool,
            Settings.EMPTY,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            entityColdStarter,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            cacheProvider
        );
    }

    public void testEmptyModelId() {
        EntityRequest request = mock(EntityRequest.class);
        when(request.getPriority()).thenReturn(RequestPriority.LOW);
        when(request.getModelId()).thenReturn(Optional.empty());
        worker.put(request);
        verify(entityColdStarter, never()).trainModel(any(), anyString(), any(), any());
        verify(request, times(1)).getModelId();
    }

    public void testOverloaded() {
        EntityRequest request = new EntityRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity);

        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah", true));

            return null;
        }).when(entityColdStarter).trainModel(any(), anyString(), any(), any());

        worker.put(request);

        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchRejectedExecutionException.class));

        // 2nd put request won't trigger anything as we are in cooldown mode
        worker.put(request);
        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
    }

    public void testException() {
        EntityRequest request = new EntityRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity);

        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onFailure(new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT));

            return null;
        }).when(entityColdStarter).trainModel(any(), anyString(), any(), any());

        worker.put(request);

        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchStatusException.class));

        // 2nd put request triggers another setException
        worker.put(request);
        verify(entityColdStarter, times(2)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(2)).setException(eq(detectorId), any(OpenSearchStatusException.class));
    }

    public void testModelHosted() {
        EntityRequest request = new EntityRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity);

        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(3);

            ADModelState<createFromValueOnlySamples> state = invocation.getArgument(2);
            state.setModel(MLUtil.createNonEmptyModel(detectorId));
            listener.onResponse(null);

            return null;
        }).when(entityColdStarter).trainModel(any(), anyString(), any(), any());

        worker.put(request);

        verify(cacheProvider, times(1)).get();
    }
}
