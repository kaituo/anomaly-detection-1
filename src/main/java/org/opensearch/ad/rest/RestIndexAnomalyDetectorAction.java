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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static org.opensearch.timeseries.util.RestHandlerUtils.IF_SEQ_NO;
import static org.opensearch.timeseries.util.RestHandlerUtils.REFRESH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * Rest handlers to create and update anomaly detector.
 */
public class RestIndexAnomalyDetectorAction extends AbstractAnomalyDetectorAction {

    private static final String INDEX_ANOMALY_DETECTOR_ACTION = "index_anomaly_detector_action";
    private final Logger logger = LogManager.getLogger(RestIndexAnomalyDetectorAction.class);
    private final Settings settings;
    private final String dataSourceIdHeader;

    public RestIndexAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
        this.settings = settings;
        this.dataSourceIdHeader = TimeSeriesSettings.DATA_SOURCE_ID_HEADER.get(settings);
    }

    @Override
    public String getName() {
        return INDEX_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetector {} action for detectorId {}", request.method(), detectorId);

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        // TODO: check detection interval < modelTTL
        AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId, null, detectionInterval, detectionWindowDelay);

        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
            ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
            : WriteRequest.RefreshPolicy.IMMEDIATE;
        RestRequest.Method method = request.getHttpRequest().method();

        if (method == RestRequest.Method.POST && detectorId != AnomalyDetector.NO_ID) {
            // reset detector to empty string detectorId is only meant for updating detector
            detectorId = AnomalyDetector.NO_ID;
        }

        maybePromoteCellIdHeaderToTransient(request, method, client);

        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(this.settings), request);
        String dataSourceId = request.header(dataSourceIdHeader);
        if (AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(this.settings) && Strings.isBlank(dataSourceId)) {
            throw new OpenSearchStatusException(dataSourceIdHeader + " header is required", RestStatus.BAD_REQUEST);
        }

        IndexAnomalyDetectorRequest indexAnomalyDetectorRequest = new IndexAnomalyDetectorRequest(
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            method,
            requestTimeout,
            maxSingleEntityDetectors,
            maxMultiEntityDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            tenantId,
            dataSourceId
        );

        return channel -> client
            .execute(IndexAnomalyDetectorAction.INSTANCE, indexAnomalyDetectorRequest, indexAnomalyDetectorResponse(channel, method));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // Create
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
                    RestRequest.Method.POST,
                    TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI
                ),
                // Update
                new ReplacedRoute(
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID)
                )
            );
    }

    /**
     * Dev/test-only: populate the EventBridge cell-id {@code ThreadContext} transient from the
    * inbound HTTP header configured by {@link AnomalyDetectorSettings#EVENT_BRIDGE_CELL_ID_HEADER_NAME}
    * when {@link AnomalyDetectorSettings#EVENT_BRIDGE_CELL_ID_FROM_HEADER} is enabled.
     *
     * <p>In production the transient is expected to be stamped by a trusted upstream gateway
     * using an authoritative source (e.g. the authenticated identity), and the toggle remains
     * {@code false}. This path exists so {@code ./gradlew run} and integration tests can exercise
     * the multi-tenant create path without a proxy. The existing transient is never overwritten,
     * so a trusted upstream always wins.
     */
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required to access the request thread context in the REST handler.")
    private void maybePromoteCellIdHeaderToTransient(RestRequest request, RestRequest.Method method, NodeClient client) {
        if (method != RestRequest.Method.POST) {
            return;
        }
        if (!AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)) {
            return;
        }
        if (!AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_FROM_HEADER.get(settings)) {
            return;
        }

        String transientKey = AnomalyDetectorSettings.EVENT_BRIDGE_CELL_ID_HEADER_NAME.get(settings);
        if (transientKey == null || transientKey.trim().isEmpty()) {
            return;
        }
        transientKey = transientKey.trim();

        org.opensearch.common.util.concurrent.ThreadContext threadContext = client.threadPool().getThreadContext();
        if (threadContext.getTransient(transientKey) != null) {
            return;
        }

        String headerValue = request.header(transientKey);
        if (headerValue == null) {
            return;
        }
        String trimmed = headerValue.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        threadContext.putTransient(transientKey, trimmed);
    }

    private RestResponseListener<IndexAnomalyDetectorResponse> indexAnomalyDetectorResponse(
        RestChannel channel,
        RestRequest.Method method
    ) {
        return new RestResponseListener<IndexAnomalyDetectorResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexAnomalyDetectorResponse response) throws Exception {
                RestStatus restStatus = RestStatus.CREATED;
                if (method == RestRequest.Method.PUT) {
                    restStatus = RestStatus.OK;
                }
                BytesRestResponse bytesRestResponse = new BytesRestResponse(
                    restStatus,
                    response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
                if (restStatus == RestStatus.CREATED) {
                    String location = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.LEGACY_AD_BASE, response.getId());
                    bytesRestResponse.addHeader("Location", location);
                }
                return bytesRestResponse;
            }
        };
    }
}
