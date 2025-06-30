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

package org.opensearch.ad.stats.suppliers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;
import static org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier.MODEL_STATE_STAT_KEYS;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class ModelsOnNodeSupplierTests extends OpenSearchTestCase {
    private ThresholdedRandomCutForest trcf;
    private List<ModelState<ThresholdedRandomCutForest>> hostedModels;
    private Clock clock;
    private List<ModelState<ThresholdedRandomCutForest>> entityModelsInformation;
    private ADPriorityCache cache;

    @Mock
    private ADCacheProvider cacheProvider;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        clock = Clock.systemUTC();
        trcf = mock(ThresholdedRandomCutForest.class);
        hostedModels = new ArrayList<>(
            Arrays
                .asList(
                    new ModelState<>(
                        trcf,
                        "rcf-model-1",
                        "detector-1",
                        null,
                        ModelManager.ModelType.RCF.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    ),
                    new ModelState<>(
                        trcf,
                        "rcf-model-2",
                        "detector-1",
                        null,
                        ModelManager.ModelType.RCF.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    )
                )
        );

        ModelState<ThresholdedRandomCutForest> entityModel1 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ModelState<ThresholdedRandomCutForest> entityModel2 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        cache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getAllModels())
            .thenReturn(Stream.concat(hostedModels.stream(), entityModelsInformation.stream()).collect(Collectors.toList()));
    }

    @Test
    public void testGet() {
        Settings settings = Settings.builder().put(AD_MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AD_MAX_MODEL_SIZE_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ADModelsOnNodeSupplier modelsOnNodeSupplier = new ADModelsOnNodeSupplier(cacheProvider, settings, clusterService);
        List<Map<String, Object>> results = modelsOnNodeSupplier.get();
        assertEquals(
            "get fails to return correct result",
            Stream
                .concat(hostedModels.stream(), entityModelsInformation.stream())
                .map(
                    modelState -> modelState
                        .getModelStateAsMap()
                        .entrySet()
                        .stream()
                        .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                        .filter(entry -> entry.getValue() != null)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )
                .collect(Collectors.toList()),
            results
        );
    }

    @Test
    public void testGetModelCount() {
        ADModelsOnNodeCountSupplier modelsOnNodeSupplier = new ADModelsOnNodeCountSupplier(cacheProvider);
        assertEquals(4L, modelsOnNodeSupplier.get().longValue());
    }

    @Test
    public void testGetForTenant() {
        when(cache.getAllModels())
            .thenReturn(
                Arrays
                    .asList(
                        modelState("model-a1", "detector-a", "tenant-a"),
                        modelState("model-b1", "detector-b", "tenant-b"),
                        modelState("model-a2", "detector-a", "tenant-a")
                    )
            );

        ADModelsOnNodeSupplier modelsOnNodeSupplier = new ADModelsOnNodeSupplier(cacheProvider, settings(), clusterService());
        List<Map<String, Object>> results = modelsOnNodeSupplier.getForTenant("tenant-a");

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(model -> "tenant-a".equals(model.get(CommonName.TENANT_ID_FIELD))));
    }

    @Test
    public void testGetModelCountForTenant() {
        when(cache.getAllModels())
            .thenReturn(
                Arrays
                    .asList(
                        modelState("model-a1", "detector-a", "tenant-a"),
                        modelState("model-b1", "detector-b", "tenant-b"),
                        modelState("model-a2", "detector-a", "tenant-a")
                    )
            );

        ADModelsOnNodeCountSupplier modelsOnNodeSupplier = new ADModelsOnNodeCountSupplier(cacheProvider);

        assertEquals(2L, modelsOnNodeSupplier.getForTenant("tenant-a").longValue());
        assertEquals(1L, modelsOnNodeSupplier.getForTenant("tenant-b").longValue());
        assertEquals(0L, modelsOnNodeSupplier.getForTenant("tenant-c").longValue());
    }

    private ModelState<ThresholdedRandomCutForest> modelState(String modelId, String detectorId, String tenantId) {
        return new ModelState<>(
            trcf,
            modelId,
            detectorId,
            tenantId,
            ModelManager.ModelType.RCF.getName(),
            clock,
            0f,
            Optional.empty(),
            new ArrayDeque<>()
        );
    }

    private Settings settings() {
        return Settings.builder().put(AD_MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
    }

    private ClusterService clusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AD_MAX_MODEL_SIZE_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }
}
