/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.sqs;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.util.Strings;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Resolves per-account queue and scheduler targets for AD EventBridge jobs.
 */
public class ADEventBridgeTargetResolver {

    private static final String ARN_PREFIX = "arn:";

    private final String region;
    private final String queueName;
    private final String scheduleManagementRoleName;
    private final String sqsDeliveryRoleName;
    private final JobQueueAccountIdProvider accountProvider;

    public ADEventBridgeTargetResolver(Settings settings, JobQueueAccountIdProvider accountProvider) {
        this.region = TimeSeriesSettings.REGION.get(settings);
        this.queueName = AnomalyDetectorSettings.SQS_QUEUE_NAME.get(settings);
        this.scheduleManagementRoleName = optionalRoleName(settings, AnomalyDetectorSettings.EVENT_BRIDGE_SCHEDULE_MANAGEMENT_ROLE_NAME);
        this.sqsDeliveryRoleName = roleName(settings, AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME);
        this.accountProvider = accountProvider;
    }

    public List<String> getAccountIds() {
        return accountProvider.getAccountIds();
    }

    public String getMaintenanceAccountId() {
        return Collections.min(getAccountIds());
    }

    public ADSqsAccountTarget resolveAccount(String accountId) {
        String normalizedAccountId = Strings.trimToNull(accountId);
        if (normalizedAccountId == null) {
            throw new IllegalArgumentException("Account ID must be provided");
        }
        Set<String> discoveredAccounts = Set.copyOf(getAccountIds());
        if (!discoveredAccounts.contains(normalizedAccountId)) {
            throw new IllegalArgumentException("Account ID [" + normalizedAccountId + "] is not in provider.getAccountIds()");
        }

        String queueArn = "arn:aws:sqs:" + region + ":" + normalizedAccountId + ":" + queueName;
        String queueUrl = "https://sqs." + region + ".amazonaws.com/" + normalizedAccountId + "/" + queueName;
        String scheduleManagementRoleArn = scheduleManagementRoleName == null
            ? null
            : buildRoleArn(normalizedAccountId, scheduleManagementRoleName);
        String schedulerRoleArn = buildRoleArn(normalizedAccountId, sqsDeliveryRoleName);
        return new ADSqsAccountTarget(normalizedAccountId, queueArn, queueUrl, scheduleManagementRoleArn, schedulerRoleArn);
    }

    public ADSqsAccountTarget resolveConfig(Config config) {
        String eventBridgeCellId = Strings.trimToNull(config.getEventBridgeCellId());
        if (eventBridgeCellId == null) {
            throw new IllegalArgumentException("Config [" + config.getId() + "] is missing " + CommonName.EVENT_BRIDGE_CELL_ID_FIELD);
        }
        return resolveAccount(eventBridgeCellId);
    }

    private static String roleName(Settings settings, org.opensearch.common.settings.Setting<String> setting) {
        String value = optionalRoleName(settings, setting);
        if (value == null) {
            throw new IllegalArgumentException(setting.getKey() + " must be configured");
        }
        return value;
    }

    private static String optionalRoleName(Settings settings, org.opensearch.common.settings.Setting<String> setting) {
        String value = Strings.trimToNull(settings.get(setting.getKey()));
        if (value == null) {
            return null;
        }
        if (value.startsWith(ARN_PREFIX)) {
            throw new IllegalArgumentException(setting.getKey() + " must be a role name, not an ARN");
        }
        return value;
    }

    private static String buildRoleArn(String accountId, String roleName) {
        return "arn:aws:iam::" + accountId + ":role/" + roleName;
    }
}
