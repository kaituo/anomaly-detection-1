/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.lease.Releasable;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ClientUtil;

/**
 * NodeStateManager is used to manage states shared by transport and ml components
 * like AnomalyDetector object
 *
 */
public class ADNodeStateManager extends NodeStateManager<ADNodeState> {
    private static final Logger LOG = LogManager.getLogger(ADNodeStateManager.class);

    /**
     * Constructor
     *
     * @param client Client to make calls to OpenSearch
     * @param xContentRegistry OS named content registry
     * @param settings OS settings
     * @param clientUtil AD Client utility
     * @param clock A UTC clock
     * @param stateTtl Max time to keep state in memory
     * @param clusterService Cluster service accessor
     */
    public ADNodeStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl,
        ClusterService clusterService
    ) {
        super(
            client,
            xContentRegistry,
            settings,
            clientUtil,
            clock,
            stateTtl,
            clusterService,
            AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            AD_BACKOFF_MINUTES
        );
    }

    /**
     * Get a detector's checkpoint and save a flag if we find any so that next time we don't need to do it again
     * @param adID  the detector's ID
     * @param listener listener to handle get request
     */
    public void getDetectorCheckpoint(String adID, ActionListener<Boolean> listener) {
        ADNodeState state = states.get(adID);
        if (state != null && state.doesCheckpointExists()) {
            listener.onResponse(Boolean.TRUE);
            return;
        }

        GetRequest request = new GetRequest(ADCommonName.CHECKPOINT_INDEX_NAME, SingleStreamModelIdMapper.getRcfModelId(adID, 0));

        clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetCheckpointResponse(adID, listener));
    }

    private ActionListener<GetResponse> onGetCheckpointResponse(String adID, ActionListener<Boolean> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Boolean.FALSE);
            } else {
                ADNodeState state = states.computeIfAbsent(adID, id -> new ADNodeState(id, clock));
                state.setCheckpointExists(true);
                listener.onResponse(Boolean.TRUE);
            }
        }, listener::onFailure);
    }

    /**
     * Clean states if it is older than our stateTtl. transportState has to be a
     * ConcurrentHashMap otherwise we will have
     * java.util.ConcurrentModificationException.
     *
     */
    @Override
    public void maintenance() {
        maintenance(states, stateTtl);
    }

    /**
     * Whether last cold start for the detector is running
     * @param adID detector ID
     * @return running or not
     */
    public boolean isColdStartRunning(String adID) {
        ADNodeState state = states.get(adID);
        if (state != null) {
            return state.isColdStartRunning();
        }

        return false;
    }

    /**
     * Mark the cold start status of the detector
     * @param adID detector ID
     * @return a callback when cold start is done
     */
    public Releasable markColdStartRunning(String adID) {
        ADNodeState state = states.computeIfAbsent(adID, id -> new ADNodeState(id, clock));
        state.setColdStartRunning(true);
        return () -> {
            ADNodeState nodeState = states.get(adID);
            if (nodeState != null) {
                nodeState.setColdStartRunning(false);
            }
        };
    }

    public void getAnomalyDetectorJob(String adID, ActionListener<Optional<AnomalyDetectorJob>> listener) {
        ADNodeState state = states.get(adID);
        if (state != null && state.getDetectorJob() != null) {
            listener.onResponse(Optional.of(state.getDetectorJob()));
        } else {
            GetRequest request = new GetRequest(CommonName.JOB_INDEX, adID);
            clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetDetectorJobResponse(adID, listener));
        }
    }

    private ActionListener<GetResponse> onGetDetectorJobResponse(String adID, ActionListener<Optional<AnomalyDetectorJob>> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Optional.empty());
                return;
            }

            String xc = response.getSourceAsString();
            LOG.debug("Fetched anomaly detector: {}", xc);

            try (
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                ADNodeState state = states.computeIfAbsent(adID, id -> new ADNodeState(id, clock));
                state.setDetectorJob(job);

                listener.onResponse(Optional.of(job));
            } catch (Exception t) {
                LOG.error(new ParameterizedMessage("Fail to parse job {}", adID), t);
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure);
    }

    @Override
    protected BiCheckedFunction<XContentParser, ? extends Config, String, IOException> getConfigParser() {
        return AnomalyDetector::parse;
    }

    @Override
    protected ADNodeState createNodeState(String configId, Clock clock) {
        return new ADNodeState(configId, clock);
    }
}
