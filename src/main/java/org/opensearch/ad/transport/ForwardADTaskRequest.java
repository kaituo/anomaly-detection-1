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

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.common.exception.VersionException;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.transport.TransportService;

public class ForwardADTaskRequest extends ActionRequest implements ToXContentObject {
    public static final String DETECTOR_FIELD = "detector";
    public static final String AD_TASK_FIELD = "ad_task";
    public static final String DETECTION_DATE_RANGE_FIELD = "detection_date_range";
    public static final String STALE_RUNNING_ENTITIES_FIELD = "stale_running_entities";
    public static final String USER_FIELD = "user";
    public static final String AVAILABLE_TASK_SLOTS_FIELD = "available_task_slots";
    public static final String AD_TASK_ACTION_FIELD = "ad_task_action";

    private AnomalyDetector detector;
    private ADTask adTask;
    private DateRange detectionDateRange;
    private List<String> staleRunningEntities;
    private User user;
    private Integer availableTaskSlots;
    private ADTaskAction adTaskAction;

    /**
     * Constructor function.
     * For most task actions, we only send ForwardADTaskRequest to node with same local AD version.
     * But it's possible that we need to clean up detector cache by sending FINISHED task action to
     * an old coordinating node when no task running for the detector.
     * Check {@link org.opensearch.ad.task.ADTaskManager#cleanDetectorCache(ADTask, TransportService, ExecutorFunction)}.
     *
     * @param detector detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param adTaskAction AD task action
     * @param availableTaskSlots available task slots
     * @param remoteAdVersion AD version of remote node
     */
    public ForwardADTaskRequest(
        AnomalyDetector detector,
        DateRange detectionDateRange,
        User user,
        ADTaskAction adTaskAction,
        Integer availableTaskSlots,
        Version remoteAdVersion
    ) {
        if (remoteAdVersion == null) {
            throw new VersionException(detector.getId(), "Can't forward AD task request to node running null AD version ");
        }
        this.detector = detector;
        this.detectionDateRange = detectionDateRange;
        this.user = user;
        this.availableTaskSlots = availableTaskSlots;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(AnomalyDetector detector, DateRange detectionDateRange, User user, ADTaskAction adTaskAction) {
        this.detector = detector;
        this.detectionDateRange = detectionDateRange;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(ADTask adTask, ADTaskAction adTaskAction) {
        this(adTask, adTaskAction, null);
    }

    public ForwardADTaskRequest(ADTask adTask, Integer availableTaskSLots, ADTaskAction adTaskAction) {
        this(adTask, adTaskAction, null);
        this.availableTaskSlots = availableTaskSLots;
    }

    public ForwardADTaskRequest(ADTask adTask, ADTaskAction adTaskAction, List<String> staleRunningEntities) {
        this.adTask = adTask;
        this.adTaskAction = adTaskAction;
        if (adTask != null) {
            this.detector = adTask.getDetector();
        }
        this.staleRunningEntities = staleRunningEntities;
    }

    public static ForwardADTaskRequest fromTaskAndDetector(ADTask adTask, AnomalyDetector detector, ADTaskAction adTaskAction) {
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, adTaskAction);
        request.detector = detector;
        return request;
    }

    public ForwardADTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(ADTaskAction.class);
        if (in.available() == 0) {
            // Old version on or before 1.0 will send less fields.
            // This will reject request from old node running AD version on or before 1.0.
            // So if coordinating node is old node, it can't use new node as worker node
            // to run task.
            throw new VersionException("Can't process ForwardADTaskRequest of old version");
        }
        if (in.readBoolean()) {
            this.adTask = new ADTask(in);
        }
        if (in.readBoolean()) {
            this.detectionDateRange = new DateRange(in);
        }
        this.staleRunningEntities = in.readOptionalStringList();
        availableTaskSlots = in.readOptionalInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        if (user != null) {
            out.writeBoolean(true);
            user.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeEnum(adTaskAction);
        // From AD 1.1, only forward AD task request to nodes with same local AD version
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalStringCollection(staleRunningEntities);
        out.writeOptionalInt(availableTaskSlots);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (detector == null) {
            validationException = addValidationError(ADCommonMessages.DETECTOR_MISSING, validationException);
        } else if (detector.getId() == null) {
            validationException = addValidationError(ADCommonMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (adTaskAction == null) {
            validationException = addValidationError(ADCommonMessages.AD_TASK_ACTION_MISSING, validationException);
        }
        if (adTaskAction == ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES && (staleRunningEntities == null || staleRunningEntities.isEmpty())) {
            validationException = addValidationError(ADCommonMessages.EMPTY_STALE_RUNNING_ENTITIES, validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public DateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public User getUser() {
        return user;
    }

    public ADTaskAction getAdTaskAction() {
        return adTaskAction;
    }

    public List<String> getStaleRunningEntities() {
        return staleRunningEntities;
    }

    public Integer getAvailableTaskSLots() {
        return availableTaskSlots;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (detector != null) {
            builder.field(DETECTOR_FIELD, detector);
        }
        if (adTask != null) {
            builder.field(AD_TASK_FIELD, adTask);
        }
        if (detectionDateRange != null) {
            builder.field(DETECTION_DATE_RANGE_FIELD, detectionDateRange);
        }
        if (staleRunningEntities != null) {
            builder.startArray(STALE_RUNNING_ENTITIES_FIELD);
            for (String entity : staleRunningEntities) {
                builder.value(entity);
            }
            builder.endArray();
        }
        if (user != null) {
            builder.field(USER_FIELD, user);
        }
        if (availableTaskSlots != null) {
            builder.field(AVAILABLE_TASK_SLOTS_FIELD, availableTaskSlots);
        }
        if (adTaskAction != null) {
            builder.field(AD_TASK_ACTION_FIELD, adTaskAction.name());
        }
        return builder.endObject();
    }

    public static ForwardADTaskRequest parse(XContentParser parser) throws IOException {
        return parseWithDetectorId(parser, null);
    }

    public static ForwardADTaskRequest parseWithDetectorId(XContentParser parser, String detectorId) throws IOException {
        AnomalyDetector detector = null;
        ADTask adTask = null;
        DateRange detectionDateRange = null;
        List<String> staleRunningEntities = null;
        User user = null;
        Integer availableTaskSlots = null;
        ADTaskAction adTaskAction = null;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DETECTOR_FIELD:
                    detector = AnomalyDetector.parse(parser, detectorId);
                    break;
                case AD_TASK_FIELD:
                    adTask = ADTask.parse(parser);
                    break;
                case DETECTION_DATE_RANGE_FIELD:
                    detectionDateRange = DateRange.parse(parser);
                    break;
                case STALE_RUNNING_ENTITIES_FIELD:
                    staleRunningEntities = parseStringList(parser);
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case AVAILABLE_TASK_SLOTS_FIELD:
                    availableTaskSlots = parser.intValue();
                    break;
                case AD_TASK_ACTION_FIELD:
                    adTaskAction = ADTaskAction.valueOf(parser.text());
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        ForwardADTaskRequest request = adTask == null
            ? new ForwardADTaskRequest(detector, detectionDateRange, user, adTaskAction)
            : new ForwardADTaskRequest(adTask, adTaskAction, staleRunningEntities);
        request.detector = detector == null ? request.detector : detector;
        request.detectionDateRange = detectionDateRange;
        request.user = user;
        request.availableTaskSlots = availableTaskSlots;
        return request;
    }

    private static List<String> parseStringList(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<String> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            values.add(parser.text());
        }
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForwardADTaskRequest request = (ForwardADTaskRequest) o;
        return Objects.equals(detector, request.detector)
            && Objects.equals(adTask, request.adTask)
            && Objects.equals(detectionDateRange, request.detectionDateRange)
            && Objects.equals(staleRunningEntities, request.staleRunningEntities)
            && Objects.equals(user, request.user)
            && Objects.equals(availableTaskSlots, request.availableTaskSlots)
            && adTaskAction == request.adTaskAction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(detector, adTask, detectionDateRange, staleRunningEntities, user, availableTaskSlots, adTaskAction);
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("detector", detector)
            .append("adTask", adTask)
            .append("detectionDateRange", detectionDateRange)
            .append("staleRunningEntities", staleRunningEntities)
            .append("user", user)
            .append("availableTaskSlots", availableTaskSlots)
            .append("adTaskAction", adTaskAction)
            .toString();
    }
}
