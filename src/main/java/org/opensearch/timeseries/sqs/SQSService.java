/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.sqs;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Service for interacting with Amazon SQS.
 * Handles queue operations like receiving messages, deleting messages, and resetting visibility.
 */
public abstract class SQSService {

    private static final Logger logger = LogManager.getLogger(SQSService.class);

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final String region;

    public static final class RedrivePolicy {
        private final String deadLetterTargetArn;
        private final int maxReceiveCount;

        public RedrivePolicy(String deadLetterTargetArn, int maxReceiveCount) {
            this.deadLetterTargetArn = deadLetterTargetArn;
            this.maxReceiveCount = maxReceiveCount;
        }

        public String getDeadLetterTargetArn() {
            return deadLetterTargetArn;
        }

        public int getMaxReceiveCount() {
            return maxReceiveCount;
        }
    }

    public SQSService(Settings settings, String queueUrl) {
        this(settings, queueUrl, null, null);
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    public SQSService(Settings settings, String queueUrl, String roleArn, String roleSessionName) {
        this.region = TimeSeriesSettings.REGION.get(settings);
        this.queueUrl = queueUrl;

        logger.info("Initializing SQS service for queue: {} in region: {}", queueUrl, region);

        this.sqsClient = AccessController.doPrivileged((PrivilegedAction<SqsClient>) () -> {
            AwsCredentialsProvider credentialsProvider = roleArn == null || roleArn.isBlank()
                ? SecurityUtil.createCredentialsProvider()
                : SecurityUtil.createAssumeRoleCredentialsProvider(region, roleArn, roleSessionName);
            return SqsClient.builder().region(Region.of(region)).credentialsProvider(credentialsProvider).build();
        });

        logger.info("SQS service initialized successfully");
    }

    /**
     * Receive messages from the SQS queue.
     *
     * @param request The receive message request
     * @return List of received messages
     */
    public List<Message> receiveMessages(ReceiveMessageRequest request) {
        try {
            ReceiveMessageResponse response = sqsClient.receiveMessage(request);
            return response.messages();
        } catch (Exception e) {
            logger.error("Error receiving messages from SQS", e);
            throw new RuntimeException("Failed to receive messages from SQS", e);
        }
    }

    /**
     * Delete a message from the SQS queue.
     *
     * @param message The message to delete
     */
    public void deleteMessage(Message message) {
        try {
            DeleteMessageRequest request = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build();

            sqsClient.deleteMessage(request);
            logger.debug("Successfully deleted message: {}", message.messageId());
        } catch (Exception e) {
            logger.error("Error deleting message: " + message.messageId(), e);
            throw new RuntimeException("Failed to delete message from SQS", e);
        }
    }

    /**
     * Reset message visibility timeout to 0, making it immediately visible for other consumers.
     *
     * @param message The message to reset visibility for
     */
    public void resetMessageVisibility(Message message) {
        try {
            ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest
                .builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .visibilityTimeout(0)
                .build();

            sqsClient.changeMessageVisibility(request);
            logger.debug("Reset visibility for message: {}", message.messageId());
        } catch (Exception e) {
            logger.error("Error resetting visibility for message: " + message.messageId(), e);
            // Don't throw exception here as this is often used in error handling
        }
    }

    /**
     * Fetch the queue's redrive policy, if configured.
     *
     * @return Redrive policy metadata or null if not configured or unavailable
     */
    public RedrivePolicy getRedrivePolicy() {
        try {
            GetQueueAttributesRequest request = GetQueueAttributesRequest
                .builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.REDRIVE_POLICY)
                .build();

            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(request);
            String redrivePolicy = response.attributes().get(QueueAttributeName.REDRIVE_POLICY);
            if (redrivePolicy == null || redrivePolicy.isBlank()) {
                return null;
            }

            Map<String, Object> parsed = XContentHelper.convertToMap(new BytesArray(redrivePolicy), false, MediaTypeRegistry.JSON).v2();
            String deadLetterTargetArn = Objects.toString(parsed.get("deadLetterTargetArn"), null);
            String maxReceiveCountValue = Objects.toString(parsed.get("maxReceiveCount"), null);
            int maxReceiveCount = parseIntOrDefault(maxReceiveCountValue, -1);

            return new RedrivePolicy(deadLetterTargetArn, maxReceiveCount);
        } catch (Exception e) {
            logger.warn("Failed to fetch redrive policy for SQS queue {}", queueUrl, e);
            return null;
        }
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Get the queue URL.
     *
     * @return The SQS queue URL
     */
    public String getQueueUrl() {
        return queueUrl;
    }

    /**
     * Get the region.
     *
     * @return The AWS region
     */
    public String getRegion() {
        return region;
    }

    /**
     * Close the SQS client and release resources.
     */
    public void close() {
        if (sqsClient != null) {
            logger.info("Closing SQS client");
            sqsClient.close();
        }
    }
}
