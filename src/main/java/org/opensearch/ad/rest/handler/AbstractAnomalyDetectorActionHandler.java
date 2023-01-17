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

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

/**
 * Abstract Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for either validating or creating anomaly detector.
 * PUT request is for updating anomaly detector.
 *
 * <p>Create, Update and Validate APIs all share similar validation process, the differences in logic
 * between the three usages of this class are outlined below.</p>
 * <ul>
 * <li><code>Create/Update:</code><p>This class is extended by <code>IndexAnomalyDetectorActionHandler</code> which handles
 *  either create AD or update AD REST Actions. When this class is constructed from these
 *  actions then the <code>isDryRun</code> parameter will be instantiated as <b>false</b>.</p>
 *  <p>This means that if the AD index doesn't exist at the time request is received it will be created.
 *  Furthermore, this handler will actually create or update the AD and also handle a few exceptions as
 *  they are thrown instead of converting some of them to ADValidationExceptions.</p>
 * <li><code>Validate:</code><p>This class is also extended by <code>ValidateAnomalyDetectorActionHandler</code> which handles
 *  the validate AD REST Actions. When this class is constructed from these
 *  actions then the <code>isDryRun</code> parameter will be instantiated as <b>true</b>.</p>
 *  <p>This means that if the AD index doesn't exist at the time request is received it wont be created.
 *  Furthermore, this means that the AD won't actually be created and all exceptions will be wrapped into
 *  DetectorValidationResponses hence the user will be notified which validation checks didn't pass.</p>
 *  <p>After completing all the first round of validation which is identical to the checks that are done for the
 *  create/update APIs, this code will check if the validation type is 'model' and if true it will
 *  instantiate the <code>ModelValidationActionHandler</code> class and run the non-blocker validation logic</p>
 *  </ul>
 */
public abstract class AbstractAnomalyDetectorActionHandler<T extends ActionResponse> extends
    AbstractTimeSeriesActionHandler<T, ADIndex, ADIndexManagement> {
    protected final Logger logger = LogManager.getLogger(AbstractAnomalyDetectorActionHandler.class);

    public static final String EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG = "Can't create more than %d HC anomaly detectors.";
    public static final String EXCEEDED_MAX_SINGLE_STREAM_DETECTORS_PREFIX_MSG =
        "Can't create more than %d single-stream anomaly detectors.";
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create anomaly detector as no document is found in the indices: ";
    public static final String DUPLICATE_DETECTOR_MSG = "Cannot create anomaly detector with name [%s] as it's already used by detector %s";
    public static final String VALIDATION_FEATURE_FAILURE = "Validation failed for feature(s) of detector %s";

    protected final Integer maxSingleStreamDetectors;
    protected final Integer maxHCAnomalyDetectors;
    protected final ADTaskManager adTaskManager;
    protected final Clock clock;
    protected final Settings settings;

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param clientUtil              AD security client
     * @param transportService        ES transport service
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleStreamAnomalyDetectors     max single-entity anomaly detectors allowed
     * @param maxHCAnomalyDetectors      max multi-entity detectors allowed
     * @param maxFeatures             max features allowed per detector
     * @param maxCategoricalFields    max number of categorical fields
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param adTaskManager           AD Task manager
     * @param searchFeatureDao        Search feature dao
     * @param validationType          Whether validation is for detector or model
     * @param isDryRun                Whether handler is dryrun or not
     * @param clock                   clock object to know when to timeout
     * @param settings                Node settings
     */
    public AbstractAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        TransportService transportService,
        ADIndexManagement anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleStreamAnomalyDetectors,
        Integer maxHCAnomalyDetectors,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        boolean isDryRun,
        Clock clock,
        Settings settings
    ) {
        super(
            anomalyDetector,
            anomalyDetectionIndices,
            isDryRun,
            client,
            detectorId,
            clientUtil,
            user,
            method,
            clusterService,
            xContentRegistry,
            transportService,
            requestTimeout,
            refreshPolicy,
            seqNo,
            primaryTerm,
            validationType,
            searchFeatureDao,
            maxFeatures,
            maxCategoricalFields
        );
        this.maxSingleStreamDetectors = maxSingleStreamAnomalyDetectors;
        this.maxHCAnomalyDetectors = maxHCAnomalyDetectors;
        this.adTaskManager = adTaskManager;
        this.clock = clock;
        this.settings = settings;
    }

    @Override
    protected TimeSeriesException createValidationException(String msg, ValidationIssueType type) {
        return new ValidationException(msg, type, ValidationAspect.DETECTOR);
    }

    @Override
    protected AnomalyDetector parse(XContentParser parser, GetResponse response) throws IOException {
        return AnomalyDetector.parse(parser, response.getId(), response.getVersion());
    }

    @Override
    protected void confirmHistoricalRunning(String id, ActionListener<Void> listener) {
        adTaskManager.getAndExecuteOnLatestDetectorLevelTask(id, HISTORICAL_DETECTOR_TASK_TYPES, (adTask) -> {
            if (adTask.isPresent() && !adTask.get().isDone()) {
                // can't update detector if there is AD task running
                listener.onFailure(new OpenSearchStatusException("Detector is running", RestStatus.INTERNAL_SERVER_ERROR));
            } else {
                listener.onResponse(null);
            }
        }, transportService, true, listener);
    }

    @Override
    protected String getExceedMaxSingleStreamConfigsErrorMsg(int maxSingleStreamConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_SINGLE_STREAM_DETECTORS_PREFIX_MSG, getMaxSingleStreamConfigs());
    }

    @Override
    protected String getExceedMaxHCConfigsErrorMsg(int maxHCConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG, getMaxHCConfigs());
    }

    @Override
    protected String getNoDocsInUserIndexErrorMsg(String suppliedIndices) {
        return String.format(Locale.ROOT, NO_DOCS_IN_USER_INDEX_MSG, suppliedIndices);
    }

    @Override
    protected String getDuplicateConfigErrorMsg(String name, List<String> otherConfigIds) {
        return String.format(Locale.ROOT, DUPLICATE_DETECTOR_MSG, name, otherConfigIds);
    }

    @Override
    protected AnomalyDetector copyConfig(User user, Config config) {
        return new AnomalyDetector(
            config.getId(),
            config.getVersion(),
            config.getName(),
            config.getDescription(),
            config.getTimeField(),
            config.getIndices(),
            config.getFeatureAttributes(),
            config.getFilterQuery(),
            config.getInterval(),
            config.getWindowDelay(),
            config.getShingleSize(),
            config.getUiMetadata(),
            config.getSchemaVersion(),
            Instant.now(),
            config.getCategoryFields(),
            user,
            config.getCustomResultIndex(),
            config.getImputationOption()
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T createIndexConfigResponse(IndexResponse indexResponse, Config config) {
        return (T) new IndexAnomalyDetectorResponse(
            indexResponse.getId(),
            indexResponse.getVersion(),
            indexResponse.getSeqNo(),
            indexResponse.getPrimaryTerm(),
            (AnomalyDetector) config,
            RestStatus.CREATED
        );
    }

    @Override
    protected Set<ValidationAspect> getDefaultValidationType() {
        return Sets.newHashSet(ValidationAspect.DETECTOR);
    }

    @Override
    protected Integer getMaxSingleStreamConfigs() {
        return maxSingleStreamDetectors;
    }

    @Override
    protected Integer getMaxHCConfigs() {
        return maxHCAnomalyDetectors;
    }

    @Override
    protected String getFeatureErrorMsg(String name) {
        return String.format(Locale.ROOT, VALIDATION_FEATURE_FAILURE, name);
    }

    @Override
    protected void validateModel(ActionListener<T> listener) {
        ModelValidationActionHandler modelValidationActionHandler = new ModelValidationActionHandler(
            clusterService,
            client,
            clientUtil,
            (ActionListener<ValidateAnomalyDetectorResponse>) listener,
            (AnomalyDetector) config,
            requestTimeout,
            xContentRegistry,
            searchFeatureDao,
            validationType,
            clock,
            settings,
            user
        );
        modelValidationActionHandler.checkIfMultiEntityDetector();
    }
}
