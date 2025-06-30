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
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.transport.EntityProfileRequest;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * REST handler to get anomaly detector entity profile.
 */
public class RestAnomalyDetectorEntityProfileAction extends BaseRestHandler {
    private static final String ANOMALY_DETECTOR_ENTITY_PROFILE_ACTION = "anomaly_detector_entity_profile_action";
    private final Settings settings;
    private final Set<String> allEntityProfileTypeStrs;
    private final Set<EntityProfileName> allEntityProfileTypes;
    private final Set<EntityProfileName> defaultEntityProfileTypes;

    public RestAnomalyDetectorEntityProfileAction(Settings settings) {
        this.settings = settings;

        List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        this.allEntityProfileTypeStrs = Name.getListStrs(allEntityProfiles);
        this.defaultEntityProfileTypes = new HashSet<>(Arrays.asList(EntityProfileName.STATE));
    }

    @Override
    public String getName() {
        return ANOMALY_DETECTOR_ENTITY_PROFILE_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RestHandlerUtils.ENTITY_PROFILE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String.format(
                        Locale.ROOT,
                        "%s/{%s}/%s/{%s}",
                        TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
                        DETECTOR_ID,
                        RestHandlerUtils.ENTITY_PROFILE,
                        TYPE
                    )
                )
            );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
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

        String typesStr = request.param(TYPE);
        boolean all = request.paramAsBoolean("_all", false);
        Set<EntityProfileName> profilesToCollect = getEntityProfilesToCollect(typesStr, all);

        Entity entity = RestHandlerUtils.buildEntity(request, detectorId);
        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);

        EntityProfileRequest entityProfileRequest = new EntityProfileRequest(detectorId, entity, profilesToCollect, tenantId);
        return channel -> client
            .execute(ADEntityProfileAction.INSTANCE, entityProfileRequest, new RestToXContentListener<>(channel));
    }

    private Set<EntityProfileName> getEntityProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return allEntityProfileTypes;
        }
        if (Strings.isEmpty(typesStr)) {
            return defaultEntityProfileTypes;
        }
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
        return EntityProfileName.getNames(Sets.intersection(allEntityProfileTypeStrs, typesInRequest));
    }
}
