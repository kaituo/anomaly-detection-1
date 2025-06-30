/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.sqs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class ADEventBridgeTargetResolverTests extends OpenSearchTestCase {

    private static final String SCHEDULE_MANAGEMENT_ROLE_NAME = "ADScheduleManagementRole";
    private static final String SQS_DELIVERY_ROLE_NAME = "EventBridgeSchedulerSqsDeliveryRole";

    public void testGetMaintenanceAccountIdUsesSmallestDiscoveredAccount() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);
        when(provider.getAccountIds()).thenReturn(List.of("793040377150", "123456789012", "900000000000"));

        ADEventBridgeTargetResolver resolver = new ADEventBridgeTargetResolver(settings(), provider);

        assertEquals("123456789012", resolver.getMaintenanceAccountId());
    }

    public void testResolveAccountBuildsDerivedTargets() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);
        when(provider.getAccountIds()).thenReturn(List.of("793040377150"));

        ADEventBridgeTargetResolver resolver = new ADEventBridgeTargetResolver(settings(), provider);

        ADSqsAccountTarget target = resolver.resolveAccount("793040377150");
        assertEquals("793040377150", target.getAccountId());
        assertEquals("arn:aws:sqs:us-west-2:793040377150:ad-jobs.fifo", target.getQueueArn());
        assertEquals("https://sqs.us-west-2.amazonaws.com/793040377150/ad-jobs.fifo", target.getQueueUrl());
        assertEquals("arn:aws:iam::793040377150:role/ADScheduleManagementRole", target.getScheduleManagementRoleArn());
        assertEquals("arn:aws:iam::793040377150:role/EventBridgeSchedulerSqsDeliveryRole", target.getSchedulerRoleArn());
    }

    public void testResolveConfigRejectsUndiscoveredAccount() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);
        when(provider.getAccountIds()).thenReturn(List.of("793040377150"));

        ADEventBridgeTargetResolver resolver = new ADEventBridgeTargetResolver(settings(), provider);

        Config config = mock(Config.class);
        when(config.getEventBridgeCellId()).thenReturn("123456789012");
        when(config.getId()).thenReturn("detector-1");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> resolver.resolveConfig(config));
        assertTrue(exception.getMessage().contains("provider.getAccountIds"));
    }

    public void testMissingScheduleManagementRoleUsesAmbientCredentials() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);
        when(provider.getAccountIds()).thenReturn(List.of("793040377150"));
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.getKey(), SQS_DELIVERY_ROLE_NAME)
            .build();

        ADEventBridgeTargetResolver resolver = new ADEventBridgeTargetResolver(settings, provider);

        ADSqsAccountTarget target = resolver.resolveAccount("793040377150");
        assertNull(target.getScheduleManagementRoleArn());
        assertEquals("arn:aws:iam::793040377150:role/EventBridgeSchedulerSqsDeliveryRole", target.getSchedulerRoleArn());
    }

    public void testRejectsMissingSqsDeliveryRoleNameSetting() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new ADEventBridgeTargetResolver(Settings.builder().put(TimeSeriesSettings.REGION.getKey(), "us-west-2").build(), provider)
        );

        assertTrue(exception.getMessage().contains(AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.getKey()));
    }

    public void testRejectsRoleArnSetting() {
        JobQueueAccountIdProvider provider = mock(JobQueueAccountIdProvider.class);
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(
                AnomalyDetectorSettings.EVENT_BRIDGE_SCHEDULE_MANAGEMENT_ROLE_NAME.getKey(),
                "arn:aws:iam::123456789012:role/ADScheduleManagementRole"
            )
            .put(AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.getKey(), SQS_DELIVERY_ROLE_NAME)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new ADEventBridgeTargetResolver(settings, provider)
        );

        assertTrue(exception.getMessage().contains("must be a role name"));
    }

    private Settings settings() {
        return Settings
            .builder()
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(AnomalyDetectorSettings.EVENT_BRIDGE_SCHEDULE_MANAGEMENT_ROLE_NAME.getKey(), SCHEDULE_MANAGEMENT_ROLE_NAME)
            .put(AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.getKey(), SQS_DELIVERY_ROLE_NAME)
            .build();
    }
}
