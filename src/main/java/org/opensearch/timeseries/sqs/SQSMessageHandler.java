/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.sqs;

import org.opensearch.core.action.ActionListener;

/**
 * Interface for handling SQS messages containing job information.
 * Implementations should process the job and return success/failure status.
 */
public interface SQSMessageHandler {

    /**
     * Process a job message from SQS.
     *
     * @param messageBody The JSON message body from SQS containing job information
     * @param listener Listener to be notified of processing result; true if successful, false for retry
     * @throws Exception if there's an unrecoverable error
     */
    void processMessage(String messageBody, ActionListener<Boolean> listener) throws Exception;

    /**
     * Get the handler name for logging purposes.
     *
     * @return handler name
     */
    String getHandlerName();
}
