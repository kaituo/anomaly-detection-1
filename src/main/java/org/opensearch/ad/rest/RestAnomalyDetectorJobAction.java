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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.STOP_JOB;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.rest.RestJobAction;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobRequest;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start/stop AD job.
 */
public class RestAnomalyDetectorJobAction extends RestJobAction {

    public static final String AD_JOB_ACTION = "anomaly_detector_job_action";
    private volatile TimeValue requestTimeout;
    private final Settings settings;

    public RestAnomalyDetectorJobAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = AD_REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_REQUEST_TIMEOUT, it -> requestTimeout = it);
        this.settings = settings;
    }

    @Override
    public String getName() {
        return AD_JOB_ACTION;
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        boolean historical = request.paramAsBoolean("historical", false);
        String rawPath = request.rawPath();
        DateRange detectionDateRange = parseInputDateRange(request);

        maybePromoteDevTestHeadersToTransients(request, client);

        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(this.settings), request);

        JobRequest anomalyDetectorJobRequest = new JobRequest(
            detectorId,
            ADIndex.CONFIG.getIndexName(),
            detectionDateRange,
            historical,
            rawPath,
            tenantId
        );

        anomalyDetectorJobRequest.getRawPath();

        return channel -> client
            .execute(AnomalyDetectorJobAction.INSTANCE, anomalyDetectorJobRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Dev/test-only: populate request transients from inbound headers when
     * {@link AnomalyDetectorSettings#EVENT_BRIDGE_CELL_ID_FROM_HEADER} is enabled.
     */
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required to access the request thread context in the REST handler.")
    private void maybePromoteDevTestHeadersToTransients(RestRequest request, NodeClient client) {
        if (!AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)) {
            return;
        }
        if (!AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_FROM_HEADER.get(settings)) {
            return;
        }

        RestHandlerUtils
            .promoteHeaderToTransient(
                request,
                client.threadPool().getThreadContext(),
                AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_HEADER_NAME.get(settings)
            );
        RestHandlerUtils
            .promoteHeaderToTransient(
                request,
                client.threadPool().getThreadContext(),
                TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.get(settings)
            );
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // start AD Job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, START_JOB),
                    RestRequest.Method.POST,
                    String
                        .format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, START_JOB)
                ),
                // stop AD Job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, STOP_JOB),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, STOP_JOB)
                )
            );
    }
}
