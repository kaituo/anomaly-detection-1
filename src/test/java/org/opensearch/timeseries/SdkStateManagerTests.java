/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.SearchModule;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

public class SdkStateManagerTests extends AbstractTimeSeriesTest {
    private static final String CONFIG_ID = "config-1";
    private static final String TENANT_ID = "tenant-1";

    private SdkStateManager stateManager;
    private ConfigDocumentStore configDocumentStore;
    private Clock clock;
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
        configDocumentStore = mock(ConfigDocumentStore.class);
        clock = mock(Clock.class);
        namedXContentRegistry = xContentRegistry();
        when(clock.instant()).thenReturn(Instant.EPOCH);
        when(clock.millis()).thenReturn(0L);
        stateManager = createStateManager(null);
    }

    private SdkStateManager createStateManager(EventBridgeHandler eventBridgeHandler) {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), 3)
            .put(TimeSeriesSettings.BACKOFF_MINUTES.getKey(), TimeValue.timeValueMinutes(10))
            .build();
        return new SdkStateManager(
            namedXContentRegistry,
            settings,
            configDocumentStore,
            clock,
            Duration.ofHours(1),
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES,
            eventBridgeHandler
        );
    }

    private void mockConfigStoreResponse(GetResponse response) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(configDocumentStore).get(any(GetRequest.class), any(TenantContext.class), any());
    }

    private void mockConfigStoreFailure(Exception exception) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(configDocumentStore).get(any(GetRequest.class), any(TenantContext.class), any());
    }

    public void testInitializeSdkClientReturnsNullWhenMultiTenancyDisabled() {
        Client client = mock(Client.class);

        assertNull(SdkStateManager.initializeSdkClient(client, namedXContentRegistry, Settings.EMPTY));
    }

    public void testInitializeSdkClientRethrowsInitializationFailure() {
        Client client = mock(Client.class);
        IllegalStateException failure = new IllegalStateException("boom");
        when(client.threadPool()).thenThrow(failure);

        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .put(AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT.getKey(), "https://remote.example.com")
            .put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "es")
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .build();

        IllegalStateException actual = expectThrows(
            IllegalStateException.class,
            () -> SdkStateManager.initializeSdkClient(client, namedXContentRegistry, settings)
        );
        assertSame(failure, actual);
    }

    public void testGetConfigReturnsDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        String configId = detector.getId();
        GetResponse getResponse = TestHelpers.createGetResponse(detector, configId, ADCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(configId, TENANT_ID, AnalysisType.AD, true, future);

        Optional<? extends Config> response = future.actionGet();
        assertTrue(response.isPresent());
        assertEquals(detector, response.get());

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).get(requestCaptor.capture(), tenantCaptor.capture(), any());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(configId, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, tenantCaptor.getValue().getTenantId());
    }

    public void testGetConfigUsesSystemWideTenantContextWhenTenantMissing() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        GetResponse getResponse = TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(detector.getId(), null, AnalysisType.AD, true, future);

        assertEquals(detector, future.actionGet().get());

        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).get(any(GetRequest.class), tenantCaptor.capture(), any());
        assertTrue(tenantCaptor.getValue().isSystemWide());
        assertNull(tenantCaptor.getValue().getTenantId());
    }

    public void testGetConfigReturnsCachedConfigWithoutStoreLookup() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        NodeState state = new NodeState(CONFIG_ID, clock);
        state.setConfigDef(detector);
        stateManager.states.put(CONFIG_ID, state);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, true, future);

        assertEquals(detector, future.actionGet().get());
        verifyNoInteractions(configDocumentStore);
    }

    public void testGetConfigReturnsEmptyWhenMissing() {
        GetResponse getResponse = mock(GetResponse.class);
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        Optional<? extends Config> response = future.actionGet();
        assertFalse(response.isPresent());
    }

    public void testGetConfigReturnsEmptyWhenStoreReturnsNullResponse() {
        mockConfigStoreResponse(null);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetConfigPropagatesStoreFailure() {
        RuntimeException failure = new RuntimeException("boom");
        mockConfigStoreFailure(failure);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        RuntimeException actual = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(actual.getMessage().contains("boom"));
    }

    public void testGetConfigReturnsEmptyWhenIndexNotFound() {
        mockConfigStoreFailure(new IndexNotFoundException(ADCommonName.CONFIG_INDEX));

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetConfigReturnsEmptyWhenIndexNotFoundOnlyAppearsInMessage() {
        mockConfigStoreFailure(new RuntimeException("[index_not_found_exception] no such index"));

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetConfigReturnsEmptyWhenCachedResponseCannotBeParsed() {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsString()).thenReturn("\"not-json\"");
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(CONFIG_ID, TENANT_ID, AnalysisType.AD, false, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetConfigFailsWhenAllFeaturesDisabled() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, false);
        GetResponse getResponse = TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(detector.getId(), TENANT_ID, AnalysisType.AD, true, future);

        EndRunException actual = expectThrows(EndRunException.class, future::actionGet);
        assertEquals(CommonMessages.ALL_FEATURES_DISABLED_ERR_MSG, actual.getMessage());
        assertTrue(actual.isEndNow());
        assertFalse(actual.isCountedInStats());
    }

    public void testGetConfigConsumerFailsOnParseError() throws Exception {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getId()).thenReturn(CONFIG_ID);
        when(getResponse.getVersion()).thenReturn(1L);
        when(getResponse.getSourceAsBytesRef()).thenReturn(new BytesArray("\"not-json\""));
        mockConfigStoreResponse(getResponse);

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

    public void testGetConfigConsumerReturnsDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        GetResponse getResponse = TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        AtomicReference<Optional<? extends Config>> response = new AtomicReference<>();
        stateManager.getConfig(detector.getId(), TENANT_ID, AnalysisType.AD, response::set, ActionListener.wrap(r -> {
            fail("expected config callback");
        }, e -> { fail(e.getMessage()); }));

        assertNotNull(response.get());
        assertTrue(response.get().isPresent());
        assertEquals(detector, response.get().get());
    }

    public void testGetForecastConfigReturnsForecaster() throws Exception {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setConfigId(CONFIG_ID).build();
        String configId = forecaster.getId();
        GetResponse getResponse = TestHelpers.createGetResponse(forecaster, configId, ForecastCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        PlainActionFuture<Optional<? extends Config>> future = PlainActionFuture.newFuture();
        stateManager.getConfig(configId, TENANT_ID, AnalysisType.FORECAST, false, future);

        Optional<? extends Config> response = future.actionGet();
        assertTrue(response.isPresent());
        assertEquals(forecaster, response.get());
    }

    public void testGetConfigConsumerReturnsForecaster() throws Exception {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setConfigId(CONFIG_ID).build();
        GetResponse getResponse = TestHelpers.createGetResponse(forecaster, forecaster.getId(), ForecastCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        AtomicReference<Optional<? extends Config>> response = new AtomicReference<>();
        stateManager.getConfig(forecaster.getId(), TENANT_ID, AnalysisType.FORECAST, response::set, ActionListener.wrap(r -> {
            fail("expected config callback");
        }, e -> { fail(e.getMessage()); }));

        assertTrue(response.get().isPresent());
        assertEquals(forecaster, response.get().get());
    }

    public void testGetConfigConsumerFailsForUnsupportedAnalysisType() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
        GetResponse getResponse = TestHelpers.createGetResponse(detector, detector.getId(), ForecastCommonName.CONFIG_INDEX);
        mockConfigStoreResponse(getResponse);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        stateManager
            .getConfig(CONFIG_ID, TENANT_ID, AnalysisType.UNKNOWN, config -> { fail("expected parse failure"); }, ActionListener.wrap(r -> {
                fail("expected parse failure");
            }, e -> {
                failure.set(e);
                latch.countDown();
            }));

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertTrue(failure.get() instanceof OpenSearchStatusException);
        assertTrue(failure.get().getMessage().contains("Failed to parse config " + CONFIG_ID));
    }

    public void testBackpressureLifecycle() {
        when(clock.millis()).thenReturn(700000L);

        assertFalse(stateManager.isMuted("node-1", CONFIG_ID));
        stateManager.addPressure("node-1", CONFIG_ID);
        stateManager.addPressure("node-1", CONFIG_ID);
        stateManager.addPressure("node-1", CONFIG_ID);
        assertFalse(stateManager.isMuted("node-1", CONFIG_ID));

        when(clock.millis()).thenReturn(700001L);
        stateManager.addPressure("node-1", CONFIG_ID);
        assertTrue(stateManager.isMuted("node-1", CONFIG_ID));

        stateManager.resetBackpressureCounter("node-1", CONFIG_ID);
        assertFalse(stateManager.isMuted("node-1", CONFIG_ID));
    }

    public void testResetBackpressureCounterWithoutRoutingIsNoOp() {
        stateManager.resetBackpressureCounter("node-1", CONFIG_ID);
        assertFalse(stateManager.isMuted("node-1", CONFIG_ID));
    }

    public void testClearRemovesStateAndBackpressure() {
        when(clock.millis()).thenReturn(1000L);
        stateManager.states.put(CONFIG_ID, new NodeState(CONFIG_ID, clock));
        for (int i = 0; i < 4; i++) {
            stateManager.addPressure("node-1", CONFIG_ID);
        }

        stateManager.clear(TENANT_ID, CONFIG_ID);

        assertFalse(stateManager.states.containsKey(CONFIG_ID));
        assertFalse(stateManager.isMuted("node-1", CONFIG_ID));
    }

    public void testMaintenanceRemovesExpiredState() {
        when(clock.instant()).thenReturn(Instant.EPOCH);
        stateManager.states.put(CONFIG_ID, new NodeState(CONFIG_ID, clock));

        when(clock.instant()).thenReturn(Instant.EPOCH.plus(Duration.ofHours(2)));
        stateManager.maintenance();

        assertFalse(stateManager.states.containsKey(CONFIG_ID));
    }

    public void testFetchExceptionAndClearReturnsStoredExceptionOnce() {
        RuntimeException failure = new RuntimeException("boom");

        stateManager.setException(CONFIG_ID, failure);

        assertSame(failure, stateManager.fetchExceptionAndClear(CONFIG_ID).get());
        assertFalse(stateManager.fetchExceptionAndClear(CONFIG_ID).isPresent());
    }

    public void testSetExceptionIgnoresBlankConfigIdAndNullException() {
        stateManager.setException("", new RuntimeException("ignored"));
        stateManager.setException(CONFIG_ID, null);

        assertTrue(stateManager.states.isEmpty());
    }

    public void testSetExceptionKeepsHigherPriorityException() {
        EndRunException existing = new EndRunException(CONFIG_ID, "stop-now", true);

        stateManager.setException(CONFIG_ID, existing);
        stateManager.setException(CONFIG_ID, new RuntimeException("later"));

        assertSame(existing, stateManager.fetchExceptionAndClear(CONFIG_ID).get());
    }

    public void testSetExceptionReplacesLowerPriorityException() {
        RuntimeException existing = new RuntimeException("existing");
        EndRunException replacement = new EndRunException(CONFIG_ID, "stop-now", true);

        stateManager.setException(CONFIG_ID, existing);
        stateManager.setException(CONFIG_ID, replacement);

        assertSame(replacement, stateManager.fetchExceptionAndClear(CONFIG_ID).get());
    }

    public void testColdStartRunningLifecycle() {
        assertFalse(stateManager.isColdStartRunning(CONFIG_ID));

        Releasable releasable = stateManager.markColdStartRunning(CONFIG_ID);
        assertTrue(stateManager.isColdStartRunning(CONFIG_ID));

        releasable.close();
        assertFalse(stateManager.isColdStartRunning(CONFIG_ID));
    }

    public void testColdStartReleaseAfterClearDoesNothing() {
        Releasable releasable = stateManager.markColdStartRunning(CONFIG_ID);
        stateManager.clear(TENANT_ID, CONFIG_ID);

        releasable.close();

        assertFalse(stateManager.isColdStartRunning(CONFIG_ID));
    }

    public void testGetJobReturnsEmptyWhenEventBridgeHandlerMissing() {
        PlainActionFuture<Optional<Job>> future = PlainActionFuture.newFuture();

        stateManager.getJob(CONFIG_ID, TENANT_ID, true, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetJobLoadsAndCachesJobFromSchedule() {
        EventBridgeHandler eventBridgeHandler = mock(EventBridgeHandler.class);
        SdkStateManager manager = createStateManager(eventBridgeHandler);
        Job job = TestHelpers.randomJob(true);
        when(eventBridgeHandler.getJobFromSchedule(TENANT_ID, CONFIG_ID)).thenReturn(Optional.of(job));

        PlainActionFuture<Optional<Job>> first = PlainActionFuture.newFuture();
        manager.getJob(CONFIG_ID, TENANT_ID, true, first);
        assertEquals(job, first.actionGet().get());

        PlainActionFuture<Optional<Job>> second = PlainActionFuture.newFuture();
        manager.getJob(CONFIG_ID, TENANT_ID, true, second);
        assertEquals(job, second.actionGet().get());

        verify(eventBridgeHandler, times(1)).getJobFromSchedule(TENANT_ID, CONFIG_ID);
    }

    public void testGetJobReturnsEmptyWhenScheduleMissing() {
        EventBridgeHandler eventBridgeHandler = mock(EventBridgeHandler.class);
        SdkStateManager manager = createStateManager(eventBridgeHandler);
        when(eventBridgeHandler.getJobFromSchedule(TENANT_ID, CONFIG_ID)).thenReturn(Optional.empty());

        PlainActionFuture<Optional<Job>> future = PlainActionFuture.newFuture();
        manager.getJob(CONFIG_ID, TENANT_ID, true, future);

        assertFalse(future.actionGet().isPresent());
    }

    public void testGetJobLoadsMaintenanceJobFromMaintenanceSchedule() {
        EventBridgeHandler eventBridgeHandler = mock(EventBridgeHandler.class);
        SdkStateManager manager = createStateManager(eventBridgeHandler);
        Job job = TestHelpers.randomJob(true);
        when(eventBridgeHandler.getMaintenanceJobFromSchedule("HourlyCron")).thenReturn(Optional.of(job));

        PlainActionFuture<Optional<Job>> future = PlainActionFuture.newFuture();
        manager.getJob("HourlyCron", TENANT_ID, true, future);

        assertEquals(job, future.actionGet().get());
        verify(eventBridgeHandler).getMaintenanceJobFromSchedule("HourlyCron");
        verify(eventBridgeHandler, times(0)).getJobFromSchedule(any(), any());
    }

    public void testGetJobReturnsEmptyWhenMaintenanceScheduleMissing() {
        EventBridgeHandler eventBridgeHandler = mock(EventBridgeHandler.class);
        SdkStateManager manager = createStateManager(eventBridgeHandler);
        when(eventBridgeHandler.getMaintenanceJobFromSchedule("DailyS3CheckpointCleanup")).thenReturn(Optional.empty());

        PlainActionFuture<Optional<Job>> future = PlainActionFuture.newFuture();
        manager.getJob("DailyS3CheckpointCleanup", TENANT_ID, true, future);

        assertFalse(future.actionGet().isPresent());
        verify(eventBridgeHandler).getMaintenanceJobFromSchedule("DailyS3CheckpointCleanup");
        verify(eventBridgeHandler, times(0)).getJobFromSchedule(any(), any());
    }

    public void testGetJobPropagatesHandlerException() throws InterruptedException {
        EventBridgeHandler eventBridgeHandler = mock(EventBridgeHandler.class);
        SdkStateManager manager = createStateManager(eventBridgeHandler);
        RuntimeException failure = new RuntimeException("boom");
        when(eventBridgeHandler.getJobFromSchedule(TENANT_ID, CONFIG_ID)).thenThrow(failure);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> thrown = new AtomicReference<>();
        manager.getJob(CONFIG_ID, TENANT_ID, true, ActionListener.wrap(r -> {
            // Current implementation also responds with Optional.empty() after onFailure.
        }, e -> {
            thrown.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertSame(failure, thrown.get());
    }
}
