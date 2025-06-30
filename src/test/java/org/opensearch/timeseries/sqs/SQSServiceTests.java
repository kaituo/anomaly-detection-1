/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.sqs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SQSServiceTests extends AbstractTimeSeriesTest {

    private static final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue";

    public void testGetRedrivePolicyReturnsNullWhenAttributeIsMissing() {
        SqsClient sqsClient = mock(SqsClient.class);
        when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenReturn(GetQueueAttributesResponse.builder().attributes(Collections.emptyMap()).build());

        assertNull(new TestSQSService(sqsClient).getRedrivePolicy());

        verify(sqsClient).getQueueAttributes(any(GetQueueAttributesRequest.class));
    }

    public void testGetRedrivePolicyThrowsWhenAttributesCannotBeFetched() {
        SqsClient sqsClient = mock(SqsClient.class);
        SqsException accessDenied = (SqsException) SqsException
            .builder()
            .awsErrorDetails(
                AwsErrorDetails
                    .builder()
                    .errorCode("AccessDenied")
                    .errorMessage("User is not authorized to perform: sqs:GetQueueAttributes")
                    .serviceName("AmazonSQS")
                    .build()
            )
            .statusCode(403)
            .build();
        when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(accessDenied);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> new TestSQSService(sqsClient).getRedrivePolicy());

        assertTrue(exception.getMessage().contains("Failed to fetch redrive policy"));
        assertTrue(exception.getMessage().contains("sqs:GetQueueAttributes"));
        assertTrue(exception.getMessage().contains("AccessDenied"));
        assertSame(accessDenied, exception.getCause());
    }

    public void testGetRedrivePolicyParsesConfiguredPolicy() {
        SqsClient sqsClient = mock(SqsClient.class);
        when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
            .thenReturn(
                GetQueueAttributesResponse
                    .builder()
                    .attributes(
                        Collections
                            .singletonMap(
                                QueueAttributeName.REDRIVE_POLICY,
                                "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-west-2:123456789012:test-dlq\",\"maxReceiveCount\":\"3\"}"
                            )
                    )
                    .build()
            );

        SQSService.RedrivePolicy redrivePolicy = new TestSQSService(sqsClient).getRedrivePolicy();

        assertEquals("arn:aws:sqs:us-west-2:123456789012:test-dlq", redrivePolicy.getDeadLetterTargetArn());
        assertEquals(3, redrivePolicy.getMaxReceiveCount());
    }

    private static class TestSQSService extends SQSService {
        private TestSQSService(SqsClient sqsClient) {
            super(Settings.builder().put(TimeSeriesSettings.REGION.getKey(), "us-west-2").build(), QUEUE_URL, sqsClient);
        }
    }
}
