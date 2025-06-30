/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.ad.rest.handler;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.sqs.ADEventBridgeTargetResolver;
import org.opensearch.ad.sqs.ADSqsAccountTarget;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DateUtils;

/**
 * AD-specific EventBridge handler that delegates multi-account target resolution to the base class.
 */
public class ADEventBridgeHandler extends EventBridgeHandler {

    public ADEventBridgeHandler(Settings settings, ClusterSettings clusterSettings, Clock clock) {
        this(settings, clock, createAccountProvider(settings));
    }

    ADEventBridgeHandler(Settings settings, Clock clock, JobQueueAccountIdProvider accountProvider) {
        super(
            TimeSeriesSettings.REGION.get(settings),
            buildPlaceholderQueueArn(TimeSeriesSettings.REGION.get(settings), AnomalyDetectorSettings.SQS_QUEUE_NAME.get(settings)),
            buildPlaceholderSchedulerRoleArn(AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.get(settings)),
            AnomalyDetectorSettings.AD_SCHEDULER_GROUP.get(settings),
            AnalysisType.AD,
            DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ.get(settings)),
            DateUtils.toDuration(AnomalyDetectorSettings.AD_DAILY_S3_CLEANUP_INTERVAL.get(settings)),
            clock,
            createMultiAccountTargetResolver(settings, accountProvider)
        );
    }

    private static MultiAccountTargetResolver createMultiAccountTargetResolver(
        Settings settings,
        JobQueueAccountIdProvider accountProvider
    ) {
        ADEventBridgeTargetResolver targetResolver = new ADEventBridgeTargetResolver(settings, accountProvider);
        return new MultiAccountTargetResolver() {
            @Override
            public EventBridgeTarget resolveConfigTarget(Config config) {
                ADSqsAccountTarget target = targetResolver.resolveConfig(config);
                return toEventBridgeTarget(target);
            }

            @Override
            public EventBridgeTarget resolveMaintenanceTarget() {
                try {
                    ADSqsAccountTarget target = targetResolver.resolveAccount(targetResolver.getMaintenanceAccountId());
                    return toEventBridgeTarget(target);
                } catch (RuntimeException e) {
                    throw new IllegalStateException("No EventBridge cell accounts discovered", e);
                }
            }

            @Override
            public EventBridgeTarget resolveAccountTarget(String accountId) {
                return toEventBridgeTarget(targetResolver.resolveAccount(accountId));
            }

            @Override
            public List<EventBridgeTarget> resolveAllTargets() {
                return targetResolver.getAccountIds().stream().map(this::resolveAccountTarget).collect(Collectors.toList());
            }

            private EventBridgeTarget toEventBridgeTarget(ADSqsAccountTarget target) {
                return new EventBridgeTarget(
                    target.getAccountId(),
                    target.getQueueArn(),
                    target.getScheduleManagementRoleArn(),
                    target.getSchedulerRoleArn()
                );
            }
        };
    }
}
