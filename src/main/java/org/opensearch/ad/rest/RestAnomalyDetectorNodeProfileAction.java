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
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Internal REST handler used by the HTTP node communicator to collect
 * local-only model profile information from a specific node.
 *
 * We cannot reuse the public detector profile API here. The public
 * /{detectorId}/profile handler rebuilds aggregated profile data through
 * GetAnomalyDetectorTransportAction/ProfileRunner, which calls
 * nodeCommunicator.profile(...) again. For the HTTP node communicator that
 * would recurse back into the same cross-node profile path instead of reading
 * only the local node state.
 */
public class RestAnomalyDetectorNodeProfileAction extends BaseRestHandler {
    public static final String NODE_PROFILE = "_node_profile";

    private static final String ACTION_NAME = "anomaly_detector_node_profile_action";

    private final Settings settings;
    private final ClusterService clusterService;
    private final Set<String> allProfileTypeStrs;
    private final Set<ProfileName> allProfileTypes;
    private final Set<ProfileName> defaultProfileTypes;

    public RestAnomalyDetectorNodeProfileAction(Settings settings, ClusterService clusterService) {
        this.settings = settings;
        this.clusterService = clusterService;

        List<ProfileName> allProfiles = Arrays.asList(ProfileName.values());
        this.allProfileTypes = EnumSet.copyOf(allProfiles);
        this.allProfileTypeStrs = Name.getListStrs(allProfiles);
        this.defaultProfileTypes = new HashSet<>(Arrays.asList(ProfileName.STATE));
    }

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, NODE_PROFILE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s/{%s}",
                            TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
                            DETECTOR_ID,
                            NODE_PROFILE,
                            TYPE
                        )
                )
            );
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);

        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        if (Strings.isEmpty(detectorId)) {
            throw new IllegalStateException(ADCommonMessages.AD_ID_MISSING_MSG);
        }

        String typesStr = request.param(TYPE);
        boolean all = request.paramAsBoolean("_all", false);
        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, getProfilesToCollect(typesStr, all), clusterService.localNode());
        profileRequest.setTenantId(tenantId);

        return channel -> client.execute(ADProfileAction.INSTANCE, profileRequest, new RestResponseListener<ProfileResponse>(channel) {
            @Override
            public org.opensearch.rest.RestResponse buildResponse(ProfileResponse response) throws Exception {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                response.toXContent(builder, channel.request());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    private Set<ProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return allProfileTypes;
        }
        if (Strings.isEmpty(typesStr)) {
            return defaultProfileTypes;
        }
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
        return ProfileName.getNames(Sets.intersection(allProfileTypeStrs, typesInRequest));
    }
}
