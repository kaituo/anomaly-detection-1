/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_PAGE_SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SinglePointFeatures;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ADResultProcessor extends
    ResultProcessor<AnomalyResultRequest, AnomalyResult, AnomalyResultResponse, ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskManager> {
    private static final Logger LOG = LogManager.getLogger(ADResultProcessor.class);

    private final ADModelManager adModelManager;

    public ADResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        float intervalRatioForRequests,
        String entityResultAction,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        NodeStateManager nodeStateManager,
        TransportService transportService,
        ADStats timeSeriesStats,
        ADTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<AnomalyResultResponse> transportResultResponseClazz,
        FeatureManager featureManager,
        ADModelManager adModelManager,
        AnalysisType context
    ) {
        super(
            requestTimeoutSetting,
            intervalRatioForRequests,
            entityResultAction,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            hashRing,
            nodeStateManager,
            transportService,
            timeSeriesStats,
            realTimeTaskManager,
            xContentRegistry,
            client,
            clientUtil,
            indexNameExpressionResolver,
            transportResultResponseClazz,
            featureManager,
            AD_MAX_ENTITIES_PER_QUERY,
            AD_PAGE_SIZE,
            context,
            false
        );
        this.adModelManager = adModelManager;
    }

    // For single stream detector
    @Override
    protected ActionListener<SinglePointFeatures> onFeatureResponseForSingleStreamConfig(
        String adID,
        Config config,
        ActionListener<AnomalyResultResponse> listener,
        String rcfModelId,
        DiscoveryNode rcfNode,
        long dataStartTime,
        long dataEndTime
    ) {
        return ActionListener.wrap(featureOptional -> {
            List<FeatureData> featureInResponse = null;
            AnomalyDetector detector = (AnomalyDetector) config;
            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = ParseUtils.getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector);
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {

                Optional<Exception> exception = coldStartIfNoCheckPoint(detector);
                if (exception.isPresent()) {
                    listener.onFailure(exception.get());
                    return;
                }

                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // and don't log to avoid bloating our logs.
                    LOG.debug("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            ResultResponse
                                .create(
                                    new ArrayList<FeatureData>(),
                                    "No data in current detection window",
                                    null,
                                    null,
                                    false,
                                    transportResultResponseClazz
                                )
                        );

                } else {
                    LOG.debug("Return at least current feature value between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            ResultResponse
                                .create(
                                    featureInResponse,
                                    "No full shingle in current detection window",
                                    null,
                                    null,
                                    false,
                                    transportResultResponseClazz
                                )
                        );
                }
                return;
            }

            final AtomicReference<Exception> failure = new AtomicReference<Exception>();

            LOG.info("Sending RCF request to {} for model {}", rcfNode.getId(), rcfModelId);

            RCFActionListener rcfListener = new RCFActionListener(
                rcfModelId,
                failure,
                rcfNode.getId(),
                detector,
                listener,
                featureInResponse,
                adID
            );

            // The threshold for splitting RCF models in single-stream detectors.
            // The smallest machine in the Amazon managed service has 1GB heap.
            // With the setting, the desired model size there is of 2 MB.
            // By default, we can have at most 5 features. Since the default shingle size
            // is 8, we have at most 40 dimensions in RCF. In our current RCF setting,
            // 30 trees, and bounding box cache ratio 0, 40 dimensions use 449KB.
            // Users can increase the number of features to 10 and shingle size to 60,
            // 30 trees, bounding box cache ratio 0, 600 dimensions use 1.8 MB.
            // Since these sizes are smaller than the threshold 2 MB, we won't split models
            // even in the smallest machine.
            transportService
                .sendRequest(
                    rcfNode,
                    RCFResultAction.NAME,
                    new RCFResultRequest(adID, rcfModelId, featureOptional.getProcessedFeatures().get()),
                    option,
                    new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
                );
        }, exception -> { handleQueryFailure(exception, listener, adID); });
    }

    // For single stream detector
    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private String modelID;
        private AtomicReference<Exception> failure;
        private String rcfNodeID;
        private Config detector;
        private ActionListener<AnomalyResultResponse> listener;
        private List<FeatureData> featureInResponse;
        private final String adID;

        RCFActionListener(
            String modelID,
            AtomicReference<Exception> failure,
            String rcfNodeID,
            Config detector,
            ActionListener<AnomalyResultResponse> listener,
            List<FeatureData> features,
            String adID
        ) {
            this.modelID = modelID;
            this.failure = failure;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.featureInResponse = features;
            this.adID = adID;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                nodeStateManager.resetBackpressureCounter(rcfNodeID, adID);
                if (response != null) {
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                response.getAnomalyGrade(),
                                response.getConfidence(),
                                response.getRCFScore(),
                                featureInResponse,
                                null,
                                response.getTotalUpdates(),
                                detector.getIntervalInMinutes(),
                                false,
                                response.getRelativeIndex(),
                                response.getAttribution(),
                                response.getPastValues(),
                                response.getExpectedValuesList(),
                                response.getLikelihoodOfValues(),
                                response.getThreshold()
                            )
                        );
                } else {
                    LOG.warn(ResultProcessor.NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                    listener.onFailure(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                ResultProcessor.handleExecuteException(ex, listener, adID);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, rcfNodeID, failure);
                Exception exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                ResultProcessor.handleExecuteException(ex, listener, adID);
            }
        }
    }

    /**
     * Verify failure of rcf or threshold models. If there is no model, trigger cold
     * start. If there is an exception for the previous cold start of this detector,
     * throw exception to the caller.
     *
     * @param failure  object that may contain exceptions thrown
     * @param detector detector object
     * @return exception if AD job execution gets resource not found exception
     * @throws Exception when the input failure is not a ResourceNotFoundException.
     *   List of exceptions we can throw
     *     1. Exception from cold start:
     *       1). InternalFailure due to
     *         a. OpenSearchTimeoutException thrown by putModelCheckpoint during cold start
     *       2). EndRunException with endNow equal to false
     *         a. training data not available
     *         b. cold start cannot succeed
     *         c. invalid training data
     *       3) EndRunException with endNow equal to true
     *         a. invalid search query
     *     2. LimitExceededException from one of RCF model node when the total size of the models
     *      is more than X% of heap memory.
     *     3. InternalFailure wrapping OpenSearchTimeoutException inside caused by
     *      RCF/Threshold model node failing to get checkpoint to restore model before timeout.
     */
    private Exception coldStartIfNoModel(AtomicReference<Exception> failure, Config detector) throws Exception {
        Exception exp = failure.get();
        if (exp == null) {
            return null;
        }

        // return exceptions like LimitExceededException to caller
        if (!(exp instanceof ResourceNotFoundException)) {
            return exp;
        }

        // fetch previous cold start exception
        String adID = detector.getId();
        final Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(adID);
        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", () -> adID, () -> exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return exception;
            }
        }
        LOG.info("Trigger cold start for {}", detector.getId());
        // only used in single-stream anomaly detector thus type cast
        coldStart((AnomalyDetector) detector);
        return previousException.orElse(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
    }

    // only used for single-stream anomaly detector
    private void coldStart(AnomalyDetector detector) {
        String detectorId = detector.getId();

        // If last cold start is not finished, we don't trigger another one
        if (nodeStateManager.isColdStartRunning(detectorId)) {
            return;
        }

        final Releasable coldStartFinishingCallback = nodeStateManager.markColdStartRunning(detectorId);

        ActionListener<Optional<double[][]>> listener = ActionListener.wrap(trainingData -> {
            if (trainingData.isPresent()) {
                double[][] dataPoints = trainingData.get();

                ActionListener<Void> trainModelListener = ActionListener
                    .wrap(res -> { LOG.info("Succeeded in training {}", detectorId); }, exception -> {
                        if (exception instanceof TimeSeriesException) {
                            // e.g., partitioned model exceeds memory limit
                            nodeStateManager.setException(detectorId, exception);
                        } else if (exception instanceof IllegalArgumentException) {
                            // IllegalArgumentException due to invalid training data
                            nodeStateManager
                                .setException(detectorId, new EndRunException(detectorId, "Invalid training data", exception, false));
                        } else if (exception instanceof OpenSearchTimeoutException) {
                            nodeStateManager
                                .setException(
                                    detectorId,
                                    new InternalFailure(detectorId, "Time out while indexing cold start checkpoint", exception)
                                );
                        } else {
                            nodeStateManager
                                .setException(detectorId, new EndRunException(detectorId, "Error while training model", exception, false));
                        }
                    });

                adModelManager
                    .trainModel(
                        detector,
                        dataPoints,
                        new ThreadedActionListener<>(
                            LOG,
                            threadPool,
                            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
                            trainModelListener,
                            false
                        )
                    );
            } else {
                nodeStateManager.setException(detectorId, new EndRunException(detectorId, "Cannot get training data", false));
            }
        }, exception -> {
            if (exception instanceof OpenSearchTimeoutException) {
                nodeStateManager
                    .setException(detectorId, new InternalFailure(detectorId, "Time out while getting training data", exception));
            } else if (exception instanceof TimeSeriesException) {
                // e.g., Invalid search query
                nodeStateManager.setException(detectorId, exception);
            } else {
                nodeStateManager.setException(detectorId, new EndRunException(detectorId, "Error while cold start", exception, false));
            }
        });

        final ActionListener<Optional<double[][]>> listenerWithReleaseCallback = ActionListener
            .runAfter(listener, coldStartFinishingCallback::close);

        threadPool
            .executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)
            .execute(
                () -> featureManager
                    .getColdStartData(
                        detector,
                        AnalysisType.AD,
                        new ThreadedActionListener<>(
                            LOG,
                            threadPool,
                            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
                            listenerWithReleaseCallback,
                            false
                        )
                    )
            );
    }

    /**
     * Check if checkpoint for an detector exists or not.  If not and previous
     *  run is not EndRunException whose endNow is true, trigger cold start.
     * @param detector detector object
     * @return previous cold start exception
     */
    private Optional<Exception> coldStartIfNoCheckPoint(AnomalyDetector detector) {
        String detectorId = detector.getId();

        Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(detectorId);

        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error(new ParameterizedMessage("Previous exception of {}:", detectorId), exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return previousException;
            }
        }

        nodeStateManager.getDetectorCheckpoint(detectorId, ActionListener.wrap(checkpointExists -> {
            if (!checkpointExists) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            }
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof IndexNotFoundException) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            } else {
                String errorMsg = String.format(Locale.ROOT, "Fail to get checkpoint state for %s", detectorId);
                LOG.error(errorMsg, exception);
                nodeStateManager.setException(detectorId, new TimeSeriesException(errorMsg, exception));
            }
        }));

        return previousException;
    }

    @Override
    protected void findException(Throwable cause, String adID, AtomicReference<Exception> failure, String nodeId) {
        if (cause == null) {
            LOG.error(new ParameterizedMessage("Null input exception"));
            return;
        }

        Exception causeException = (Exception) cause;

        if (causeException instanceof IndexNotFoundException && causeException.getMessage().contains(ADCommonName.CHECKPOINT_INDEX_NAME)) {
            // checkpoint index does not exist
            // ResourceNotFoundException will trigger cold start later
            failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
        }
        super.findException(cause, adID, failure, nodeId);
    }
}
