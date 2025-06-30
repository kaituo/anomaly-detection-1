/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.rest.handler.Processor;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class BaseValidateConfigTransportActionTests extends OpenSearchTestCase {

    private static final String TENANT_ID = "account-id:application-id:workspace-id";

    private DataAccess dataAccess;
    private TestValidateConfigTransportAction action;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dataAccess = mock(DataAccess.class);
        ClusterService clusterService = mock(ClusterService.class);
        Set<Setting<?>> clusterSettingsSet = new HashSet<>();
        clusterSettingsSet.add(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, clusterSettingsSet));
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true).build();

        action = new TestValidateConfigTransportAction(clusterService, settings, dataAccess, new ImmediateRunContext());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRequestTenantIdIsStampedOnConfigBeforeSourceIndexSearch() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        detector.setTenantId(null);
        ValidateConfigRequest request = new ValidateConfigRequest(
            AnalysisType.AD,
            detector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new org.opensearch.common.unit.TimeValue(5_000L),
            10,
            TENANT_ID
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(dataAccess).search(any(SearchRequest.class), any(TenantContext.class), any(ActionListener.class));

        PlainActionFuture<ValidateConfigResponse> future = PlainActionFuture.newFuture();
        action.executeForTest(request, future);

        ValidateConfigResponse response = future.actionGet(5_000L);
        assertNull(response.getIssue());
        assertEquals(TENANT_ID, detector.getTenantId());

        ArgumentCaptor<TenantContext> tenantContextCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(dataAccess).search(any(SearchRequest.class), tenantContextCaptor.capture(), any(ActionListener.class));
        assertEquals(TENANT_ID, tenantContextCaptor.getValue().getTenantId());
    }

    private static class TestValidateConfigTransportAction extends
        BaseValidateConfigTransportAction<ADIndex, ADDelegatingDataManagement, ADDelegatingDataManagement> {

        @SuppressWarnings("unchecked")
        TestValidateConfigTransportAction(ClusterService clusterService, Settings settings, DataAccess dataAccess, RunContext runContext) {
            super(
                "test:validate-config",
                clusterService,
                NamedXContentRegistry.EMPTY,
                settings,
                mock(ADDelegatingDataManagement.class),
                mock(ActionFilters.class),
                mock(TransportService.class),
                mock(SearchFeatureDao.class),
                AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                ValidationAspect.DETECTOR,
                mock(ADDelegatingDataManagement.class),
                new NamedWriteableRegistry(Collections.emptyList()),
                dataAccess,
                runContext
            );
        }

        private void executeForTest(ValidateConfigRequest request, ActionListener<ValidateConfigResponse> listener) {
            doExecute(mock(Task.class), request, listener);
        }

        @Override
        protected Processor<ValidateConfigResponse> createProcessor(Config config, ValidateConfigRequest request, User user) {
            return listener -> listener.onResponse(new ValidateConfigResponse((ConfigValidationIssue) null));
        }

        @Override
        protected Setting<Boolean> getMultiTenancyEnabledSetting() {
            return AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
        }
    }

    private static class ImmediateRunContext implements RunContext {
        @Override
        public void runWithSystemAuth(CheckedRunnable<Exception> action, Consumer<Exception> onFailure) {
            try {
                action.run();
            } catch (Exception e) {
                onFailure.accept(e);
            }
        }

        @Override
        public void runWithSystemAuth(CheckedConsumer<RestorableContext, Exception> action, Consumer<Exception> onFailure) {
            try {
                action.accept(() -> {});
            } catch (Exception e) {
                onFailure.accept(e);
            }
        }

        @Override
        public User getUser() {
            return null;
        }
    }
}
