/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.search.SearchModule;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

public class SdkStateManagerTests extends AbstractTimeSeriesTest {
    private static final String CONFIG_ID = "config-1";
    private static final String TENANT_ID = "tenant-1";

    private SdkStateManager stateManager;
    private SdkClient sdkClient;
    private Clock clock;
    private Client client;
    private NamedXContentRegistry namedXContentRegistry;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        sdkClient = mock(SdkClient.class);
        clock = mock(Clock.class);
        client = mock(Client.class);
        namedXContentRegistry = xContentRegistry();
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), 3)
            .put(TimeSeriesSettings.BACKOFF_MINUTES.getKey(), TimeValue.timeValueMinutes(10))
            .build();
        stateManager = new SdkStateManager(
            client,
            namedXContentRegistry,
            settings,
            clock,
            Duration.ofHours(1),
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES,
            null
        ) {
            @Override
            protected SdkClient initializeSdkClient(Settings settings) {
                return sdkClient;
            }
        };
    }

    public void testGetConfigReturnsDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        String configId = detector.getId();
        GetResponse getResponse = TestHelpers.createGetResponse(detector, configId, ADCommonName.CONFIG_INDEX);
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new GetDataObjectResponse(getResponse)));

        org.opensearch.action.support.PlainActionFuture<Optional<? extends Config>> future = org.opensearch.action.support.PlainActionFuture
            .newFuture();
        stateManager.getConfig(configId, TENANT_ID, AnalysisType.AD, true, future);

        Optional<? extends Config> response = future.actionGet();
        assertTrue(response.isPresent());
        assertEquals(detector, response.get());

        ArgumentCaptor<GetDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetDataObjectRequest.class);
        verify(sdkClient).getDataObjectAsync(requestCaptor.capture());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(configId, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
    }

    public void testGetConfigReturnsEmptyWhenMissing() {
        GetDataObjectResponse sdkResponse = mock(GetDataObjectResponse.class);
        String missingResponse = "{\"_index\":\"" + ADCommonName.CONFIG_INDEX + "\",\"_id\":\"" + CONFIG_ID + "\",\"found\":false}";
        try {
            when(sdkResponse.parser())
                .thenReturn(
                    XContentType.JSON.xContent().createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, missingResponse)
                );
        } catch (Exception e) {
            fail("failed to create mocked SDK response parser");
        }
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class))).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        org.opensearch.action.support.PlainActionFuture<Optional<? extends Config>> future = org.opensearch.action.support.PlainActionFuture
            .newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        Optional<? extends Config> response = future.actionGet();
        assertFalse(response.isPresent());
    }

    public void testGetConfigPropagatesStoreFailure() {
        RuntimeException failure = new RuntimeException("boom");
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));

        org.opensearch.action.support.PlainActionFuture<Optional<? extends Config>> future = org.opensearch.action.support.PlainActionFuture
            .newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        RuntimeException actual = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(actual.getMessage().contains("boom"));
    }

    public void testGetConfigConsumerFailsOnParseError() throws Exception {
        GetDataObjectResponse sdkResponse = mock(GetDataObjectResponse.class);
        String invalidSourceResponse = "{\"_index\":\""
            + ADCommonName.CONFIG_INDEX
            + "\",\"_id\":\""
            + CONFIG_ID
            + "\",\"_version\":1,\"found\":true,\"_source\":\"not-json\"}";
        when(sdkResponse.parser())
            .thenReturn(
                XContentType.JSON.xContent().createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, invalidSourceResponse)
            );
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class))).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        stateManager
            .getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, config -> { fail("expected parse failure"); }, ActionListener.wrap(r -> {
                fail("expected parse failure");
            }, e -> {
                failure.set(e);
                latch.countDown();
            }));

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertTrue(failure.get() instanceof OpenSearchStatusException);
        assertTrue(failure.get().getMessage().contains("Failed to parse config " + CONFIG_ID));
    }

    public void testGetForecastConfigReturnsForecaster() throws Exception {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setConfigId(CONFIG_ID).build();
        String configId = forecaster.getId();
        GetResponse getResponse = TestHelpers
            .createGetResponse(forecaster, configId, org.opensearch.forecast.constant.ForecastCommonName.CONFIG_INDEX);
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new GetDataObjectResponse(getResponse)));

        org.opensearch.action.support.PlainActionFuture<Optional<? extends Config>> future = org.opensearch.action.support.PlainActionFuture
            .newFuture();
        stateManager.getConfig(configId, TENANT_ID, AnalysisType.FORECAST, false, future);

        Optional<? extends Config> response = future.actionGet();
        assertTrue(response.isPresent());
        assertEquals(forecaster, response.get());
    }
}
