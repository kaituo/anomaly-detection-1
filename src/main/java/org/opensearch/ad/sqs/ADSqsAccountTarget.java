/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.sqs;

/**
 * Derived EventBridge and SQS routing information for one AWS account.
 */
public final class ADSqsAccountTarget {

    private final String accountId;
    private final String queueArn;
    private final String queueUrl;
    private final String scheduleManagementRoleArn;
    private final String scheduleManagementExternalId;
    private final String schedulerRoleArn;

    public ADSqsAccountTarget(String accountId, String queueArn, String queueUrl, String schedulerRoleArn) {
        this(accountId, queueArn, queueUrl, null, null, schedulerRoleArn);
    }

    public ADSqsAccountTarget(
        String accountId,
        String queueArn,
        String queueUrl,
        String scheduleManagementRoleArn,
        String schedulerRoleArn
    ) {
        this(accountId, queueArn, queueUrl, scheduleManagementRoleArn, null, schedulerRoleArn);
    }

    public ADSqsAccountTarget(
        String accountId,
        String queueArn,
        String queueUrl,
        String scheduleManagementRoleArn,
        String scheduleManagementExternalId,
        String schedulerRoleArn
    ) {
        this.accountId = accountId;
        this.queueArn = queueArn;
        this.queueUrl = queueUrl;
        this.scheduleManagementRoleArn = scheduleManagementRoleArn;
        this.scheduleManagementExternalId = scheduleManagementExternalId;
        this.schedulerRoleArn = schedulerRoleArn;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getQueueArn() {
        return queueArn;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public String getScheduleManagementRoleArn() {
        return scheduleManagementRoleArn;
    }

    public String getScheduleManagementExternalId() {
        return scheduleManagementExternalId;
    }

    public String getSchedulerRoleArn() {
        return schedulerRoleArn;
    }
}
