/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.CronAction;
import org.opensearch.timeseries.transport.CronRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestCronActionTests extends OpenSearchTestCase {

    public void testRoutesUseInternalCronPath() {
        RestCronAction action = new RestCronAction(multiTenantSettings());

        assertEquals(TimeSeriesAnalyticsPlugin.TIMESERIES_BASE_URI + "/_internal/_cron", RestCronAction.CRON_URI);
        assertEquals(1, action.routes().size());
        Route route = action.routes().get(0);
        assertEquals(RestRequest.Method.POST, route.getMethod());
        assertEquals(RestCronAction.CRON_URI, route.getPath());
    }

    public void testPrepareRequestRequiresInternalToken() {
        RestCronAction action = new RestCronAction(multiTenantSettings());

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> action.prepareRequest(createRequest(Map.of()), mock(NodeClient.class))
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Missing or invalid internal API token", exception.getMessage());
    }

    public void testPrepareRequestRejectsInvalidInternalToken() {
        RestCronAction action = new RestCronAction(multiTenantSettings());

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(Map.of(CommonName.INTERNAL_API_TOKEN_HEADER, List.of("wrong-secret"))),
                    mock(NodeClient.class)
                )
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Missing or invalid internal API token", exception.getMessage());
    }

    public void testPrepareRequestAcceptsValidInternalToken() throws Exception {
        TestRestCronAction action = new TestRestCronAction(multiTenantSettings());
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action
            .executePreparedRequest(
                createRequest(Map.of(CommonName.INTERNAL_API_TOKEN_HEADER, List.of("test-secret"))),
                client,
                channel
            );

        verify(client).execute(eq(CronAction.INSTANCE), any(CronRequest.class), any());
    }

    private Settings multiTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .put(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey(), "test-secret")
            .build();
    }

    private FakeRestRequest createRequest(Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath(RestCronAction.CRON_URI);
        builder.withHeaders(new HashMap<>(headers));
        return builder.build();
    }

    private static class TestRestCronAction extends RestCronAction {
        TestRestCronAction(Settings settings) {
            super(settings);
        }

        void executePreparedRequest(RestRequest request, NodeClient client, RestChannel channel) throws Exception {
            prepareRequest(request, client).accept(channel);
        }
    }
}
