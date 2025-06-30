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

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.util.List;
import java.util.Locale;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.DeleteADModelAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.DeleteModelRequest;
import org.opensearch.timeseries.transport.DeleteModelResponse;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for deleting AD models on a single node.
 * This endpoint is used by the HTTP node communicator in multi-tenant mode.
 */
public class RestDeleteADModelAction extends BaseRestHandler {
    private static final String DELETE_AD_MODEL_ACTION = "delete_ad_model_action";
    private final Settings settings;
    private final ClusterService clusterService;

    public RestDeleteADModelAction(Settings settings, ClusterService clusterService) {
        this.settings = settings;
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return DELETE_AD_MODEL_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
                            DETECTOR_ID,
                            RestHandlerUtils.DELETE_MODEL
                        )
                )
            );
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);

        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        if (!request.hasParam(DETECTOR_ID)) {
            throw new IllegalStateException(ADCommonMessages.AD_ID_MISSING_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        if (Strings.isEmpty(detectorId)) {
            throw new IllegalStateException(ADCommonMessages.AD_ID_MISSING_MSG);
        }

        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        DiscoveryNode[] nodes = new DiscoveryNode[] { clusterService.localNode() };
        DeleteModelRequest deleteRequest = new DeleteModelRequest(detectorId, tenantId, nodes);

        return channel -> client.execute(DeleteADModelAction.INSTANCE, deleteRequest, deleteADModelResponse(channel));
    }

    /**
     * Builds a response listener for the delete AD model action.
     * 
     * RestToXContentListener requires ToXContentObject, but DeleteModelResponse is ToXContentFragment.
     * Serialize DeleteModelResponse (a ToXContentFragment) by wrapping it with startObject()/endObject() and returning 200 OK.
     *
     * @param channel The REST channel to send the response to.
     * @return A response listener for the delete AD model action.
     */
    private RestResponseListener<DeleteModelResponse> deleteADModelResponse(RestChannel channel) {
        return new RestResponseListener<DeleteModelResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteModelResponse response) throws Exception {
                return new BytesRestResponse(
                    RestStatus.OK,
                    response.toXContent(channel.newBuilder().startObject(), ToXContent.EMPTY_PARAMS).endObject()
                );
            }
        };
    }
}
