/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.SdkRunContext;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Job;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

public class DeleteAnomalyDetectorTests extends AbstractTimeSeriesTest {
    private DeleteAnomalyDetectorTransportAction action;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private DataAccess taskSearcher;
    private ADTaskManager adTaskManager;
    private ADDelegatingDataManagement dataManagement;
    private PlainActionFuture<DeleteResponse> future;
    private DeleteResponse deleteResponse;
    ClusterService clusterService;
    private StateManager nodeStatemanager;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(EntityProfileTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        actionFilters = mock(ActionFilters.class);
        adTaskManager = mock(ADTaskManager.class);
        taskSearcher = mock(DataAccess.class);
        when(adTaskManager.getDataAccess()).thenReturn(taskSearcher);
        dataManagement = mock(ADDelegatingDataManagement.class);
        when(dataManagement.doesJobIndexExist()).thenReturn(false);
        nodeStatemanager = mock(StateManager.class);
        RunContext runContext = new SdkRunContext();
        action = new DeleteAnomalyDetectorTransportAction(
            transportService,
            actionFilters,
            clusterService,
            Settings.EMPTY,
            xContentRegistry(),
            nodeStatemanager,
            adTaskManager,
            dataManagement,
            runContext
        );
    }

    public void testDeleteADTransportAction_FailDeleteResponse() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(true, true, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any(), any());
        verify(taskSearcher, times(1)).delete(any(), any(), any());
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_NullAnomalyDetector() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(true, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any(), any());
        verify(taskSearcher, times(3)).delete(any(), any(), any());
    }

    public void testDeleteADTransportAction_DeleteResponseException() {
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(true, false, true, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteTasks(eq("1234"), any(), any(), any());
        verify(taskSearcher, times(1)).delete(any(), any(), any());
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_LatestDetectorLevelTask() {
        when(clusterService.state()).thenReturn(createClusterState());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> consumer = (Consumer<Optional<ADTask>>) args[3];
            ADTask adTask = ADTask.builder().state("RUNNING").build();
            consumer.accept(Optional.of(adTask));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(eq("1234"), any(), any(), any(), eq(transportService), eq(false), any());

        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_JobRunning() {
        when(clusterService.state()).thenReturn(createClusterState());
        when(dataManagement.doesJobIndexExist()).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Job>> listener = (ActionListener<Optional<Job>>) args[3];
            Job job = mock(Job.class);
            when(job.isEnabled()).thenReturn(true);
            listener.onResponse(Optional.of(job));
            return null;
        }).when(nodeStatemanager).getJob(eq("1234"), any(), eq(false), any());

        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_GetResponseException() {
        when(clusterService.state()).thenReturn(createClusterState());
        future = mock(PlainActionFuture.class);
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName(), null);
        setupMocks(false, false, false, true);

        action.doExecute(mock(Task.class), request, future);
        verify(nodeStatemanager).getJob(anyString(), any(), eq(false), any());
        verify(future).onFailure(any(RuntimeException.class));
    }

    private ClusterState createClusterState() {
        Map<String, IndexMetadata> immutableOpenMap = new HashMap<>();
        immutableOpenMap
            .put(
                CommonName.JOB_INDEX,
                IndexMetadata
                    .builder("test")
                    .settings(
                        Settings
                            .builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put("index.version.created", Version.CURRENT.id)
                    )
                    .build()
            );
        Metadata metaData = Metadata.builder().indices(immutableOpenMap).build();
        ClusterState clusterState = new ClusterState(
            new ClusterName("test_name"),
            1l,
            "uuid",
            metaData,
            null,
            null,
            null,
            new HashMap<>(),
            1,
            true
        );
        return clusterState;
    }

    private void setupMocks(
        boolean nullAnomalyDetectorResponse,
        boolean failDeleteDeleteResponse,
        boolean deleteResponseException,
        boolean getResponseFailure
    ) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<AnomalyDetector>> consumer = (Consumer<Optional<AnomalyDetector>>) args[3];
            if (nullAnomalyDetectorResponse) {
                consumer.accept(Optional.empty());
            } else {
                AnomalyDetector ad = mock(AnomalyDetector.class);
                consumer.accept(Optional.of(ad));
            }
            return null;
        }).when(nodeStatemanager).getConfig(any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ExecutorFunction function = (ExecutorFunction) args[1];

            function.execute();
            return null;
        }).when(adTaskManager).deleteTasks(eq("1234"), any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<DeleteResponse> listener = (ActionListener<DeleteResponse>) args[2];
            deleteResponse = mock(DeleteResponse.class);
            if (deleteResponseException) {
                listener.onFailure(new RuntimeException("Failed to delete anomaly detector job"));
                return null;
            }
            if (failDeleteDeleteResponse) {
                doReturn(DocWriteResponse.Result.CREATED).when(deleteResponse).getResult();
            } else {
                doReturn(DocWriteResponse.Result.DELETED).when(deleteResponse).getResult();
            }
            listener.onResponse(deleteResponse);
            return null;
        }).when(taskSearcher).delete(any(), any(), any());

        if (getResponseFailure) {
            when(dataManagement.doesJobIndexExist()).thenReturn(true);
            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                ActionListener<Optional<Job>> listener = (ActionListener<Optional<Job>>) args[3];
                listener.onFailure(new RuntimeException("Fail to get anomaly detector job"));
                return null;
            }).when(nodeStatemanager).getJob(anyString(), any(), eq(false), any());
        }
    }
}
