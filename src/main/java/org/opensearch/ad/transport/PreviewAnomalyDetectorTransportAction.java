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

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_PREVIEW_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public class PreviewAnomalyDetectorTransportAction extends
    HandledTransportAction<PreviewAnomalyDetectorRequest, PreviewAnomalyDetectorResponse> {
    private final Logger logger = LogManager.getLogger(PreviewAnomalyDetectorTransportAction.class);
    private final AnomalyDetectorRunner anomalyDetectorRunner;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Integer maxAnomalyFeatures;
    private volatile Boolean filterByEnabled;
    private final CircuitBreakerService adCircuitBreakerService;
    private Semaphore lock;
    private final StateManager stateManager;
    private final ADDelegatingDataManagement dataManagement;
    private final RunContext runContext;

    @Inject
    public PreviewAnomalyDetectorTransportAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        AnomalyDetectorRunner anomalyDetectorRunner,
        NamedXContentRegistry xContentRegistry,
        CircuitBreakerService adCircuitBreakerService,
        StateManager stateManager,
        ADDelegatingDataManagement dataManagement,
        RunContext runContext
    ) {
        super(PreviewAnomalyDetectorAction.NAME, transportService, actionFilters, PreviewAnomalyDetectorRequest::new);
        this.anomalyDetectorRunner = anomalyDetectorRunner;
        this.xContentRegistry = xContentRegistry;
        maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_FEATURES, it -> maxAnomalyFeatures = it);
        filterByEnabled = AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.lock = new Semaphore(MAX_CONCURRENT_PREVIEW.get(settings), true);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CONCURRENT_PREVIEW, it -> { lock = new Semaphore(it); });
        this.stateManager = stateManager;
        this.dataManagement = dataManagement;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(
        Task task,
        PreviewAnomalyDetectorRequest request,
        ActionListener<PreviewAnomalyDetectorResponse> actionListener
    ) {
        String detectorId = request.getId();
        User user = runContext.getUser();
        ActionListener<PreviewAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_PREVIEW_DETECTOR);

        runContext
            .runWithSystemAuth(
                context -> verifyResourceAccessAndProcessRequest(
                    ADCommonName.AD_RESOURCE_TYPE,
                    () -> previewExecute(request, context, listener),
                    () -> resolveUserAndExecute(
                        user,
                        detectorId,
                        filterByEnabled,
                        listener,
                        ad -> previewExecute(request, context, listener),
                        xContentRegistry,
                        stateManager,
                        dataManagement,
                        request.getTenantId(),
                        AnomalyDetector.class
                    )
                ),
                exception -> {
                    logger.error(exception);
                    listener.onFailure(exception);
                }
            );
    }

    void previewExecute(
        PreviewAnomalyDetectorRequest request,
        RunContext.RestorableContext context,
        ActionListener<PreviewAnomalyDetectorResponse> listener
    ) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }
        try {
            if (!lock.tryAcquire()) {
                listener.onFailure(new ClientException(request.getId(), ADCommonMessages.REQUEST_THROTTLED_MSG));
                return;
            }

            try {
                AnomalyDetector detector = request.getDetector();
                String detectorId = request.getId();
                Instant startTime = request.getStartTime();
                Instant endTime = request.getEndTime();
                ActionListener<PreviewAnomalyDetectorResponse> releaseListener = ActionListener.runAfter(listener, () -> lock.release());
                if (detector != null) {
                    if (detector.getTenantId() == null && request.getTenantId() != null) {
                        detector.setTenantId(request.getTenantId());
                    }
                    String error = validateDetector(detector);
                    if (StringUtils.isNotBlank(error)) {
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
                        lock.release();
                        return;
                    }
                    anomalyDetectorRunner
                        .executeDetector(
                            detector,
                            startTime,
                            endTime,
                            context,
                            getPreviewDetectorActionListener(releaseListener, detector)
                        );
                } else {
                    previewAnomalyDetector(releaseListener, detectorId, detector, startTime, endTime, context, request.getTenantId());
                }
            } catch (Exception e) {
                logger.error("Fail to preview", e);
                lock.release();
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private String validateDetector(AnomalyDetector detector) {
        if (detector.getFeatureAttributes().isEmpty()) {
            return "Can't preview detector without feature";
        } else {
            return RestHandlerUtils.checkFeaturesSyntax(detector, maxAnomalyFeatures);
        }
    }

    private ActionListener<List<AnomalyResult>> getPreviewDetectorActionListener(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        AnomalyDetector detector
    ) {
        return ActionListener.wrap(new CheckedConsumer<List<AnomalyResult>, Exception>() {
            @Override
            public void accept(List<AnomalyResult> anomalyResult) throws Exception {
                PreviewAnomalyDetectorResponse response = new PreviewAnomalyDetectorResponse(anomalyResult, detector);
                listener.onResponse(response);
            }
        }, exception -> {
            logger.error("Unexpected error running anomaly detector " + detector.getId(), exception);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Unexpected error running anomaly detector " + detector.getId() + ". " + exception.getMessage(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        });
    }

    private void previewAnomalyDetector(
        ActionListener<PreviewAnomalyDetectorResponse> listener,
        String detectorId,
        AnomalyDetector detector,
        Instant startTime,
        Instant endTime,
        RunContext.RestorableContext context,
        String tenantId
    ) throws IOException {
        if (!StringUtils.isBlank(detectorId)) {
            stateManager.getConfig(detectorId, tenantId, AnalysisType.AD, false, ActionListener.wrap(detectorOptional -> {
                if (detectorOptional.isEmpty()) {
                    listener
                        .onFailure(
                            new OpenSearchStatusException("Can't find anomaly detector with id:" + detectorId, RestStatus.NOT_FOUND)
                        );
                    return;
                }
                AnomalyDetector existing = (AnomalyDetector) detectorOptional.get();
                anomalyDetectorRunner
                    .executeDetector(existing, startTime, endTime, context, getPreviewDetectorActionListener(listener, existing));
            }, exception -> { listener.onFailure(new TimeSeriesException("Could not execute get query to find detector")); }));
        } else {
            anomalyDetectorRunner
                .executeDetector(detector, startTime, endTime, context, getPreviewDetectorActionListener(listener, detector));
        }
    }
}
