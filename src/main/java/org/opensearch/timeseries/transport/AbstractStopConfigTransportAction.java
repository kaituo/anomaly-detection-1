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

package org.opensearch.timeseries.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.transport.TransportService;

public abstract class AbstractStopConfigTransportAction extends HandledTransportAction<ActionRequest, StopConfigResponse> {

    private final NodeCommunicator nodeCommunicator;
    private final Logger logger;
    private final String failToStopMessage;
    private final String deleteAllModelsMessage;
    private final String deleteSuccessMessage;
    private final String deleteExceptionMessage;

    protected AbstractStopConfigTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeCommunicator nodeCommunicator,
        Logger logger,
        String failToStopMessage,
        String deleteAllModelsMessage,
        String deleteSuccessMessage,
        String deleteExceptionMessage
    ) {
        super(actionName, transportService, actionFilters, StopConfigRequest::new);
        this.nodeCommunicator = nodeCommunicator;
        this.logger = logger;
        this.failToStopMessage = failToStopMessage;
        this.deleteAllModelsMessage = deleteAllModelsMessage;
        this.deleteSuccessMessage = deleteSuccessMessage;
        this.deleteExceptionMessage = deleteExceptionMessage;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<StopConfigResponse> listener) {
        StopConfigRequest request = StopConfigRequest.fromActionRequest(actionRequest);
        String configId = request.getConfigID();
        String tenantId = request.getTenantId();
        try {
            DeleteModelRequest modelDeleteRequest = new DeleteModelRequest(configId, tenantId);
            nodeCommunicator.deleteModel(modelDeleteRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    logger.warn(deleteAllModelsMessage, configId);
                    for (FailedNodeException failedNodeException : response.failures()) {
                        logger.warn("Deleting models of node has exception", failedNodeException);
                    }
                    // if customers are using an updated detector and we haven't deleted old
                    // checkpoints, customer would have trouble
                    listener.onResponse(new StopConfigResponse(false));
                } else {
                    logger.info(deleteSuccessMessage, configId);
                    listener.onResponse(new StopConfigResponse(true));
                }
            }, exception -> {
                logger.error(new ParameterizedMessage(deleteExceptionMessage, configId), exception);
                listener.onResponse(new StopConfigResponse(false));
            }));
        } catch (Exception e) {
            logger.error(failToStopMessage + " " + configId, e);
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            listener.onFailure(new InternalFailure(configId, failToStopMessage, cause));
        }
    }
}
