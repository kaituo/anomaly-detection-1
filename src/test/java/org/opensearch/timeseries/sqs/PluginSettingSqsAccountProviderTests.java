/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.sqs;

import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class PluginSettingSqsAccountProviderTests extends OpenSearchTestCase {

    public void testGetTypeReturnsPluginSetting() {
        assertEquals("plugin_setting", new PluginSettingSqsAccountProvider().getType());
    }

    public void testGetAccountIdsReturnsNormalizedOrderedList() {
        PluginSettingSqsAccountProvider provider = new PluginSettingSqsAccountProvider();

        provider
            .initialize(
                Settings
                    .builder()
                    .putList(TimeSeriesSettings.SQS_ACCOUNT_IDS.getKey(), " 793040377150 ", "", "123456789012", "793040377150")
                    .build()
            );

        assertEquals(List.of("793040377150", "123456789012"), provider.getAccountIds());
    }

    public void testFindCreatesPluginSettingProvider() {
        JobQueueAccountIdProvider provider = JobQueueAccountIdProvider
            .find(
                "plugin_setting",
                Settings.builder().putList(TimeSeriesSettings.SQS_ACCOUNT_IDS.getKey(), "222222222222", "111111111111").build()
            );

        assertEquals("plugin_setting", provider.getType());
        assertEquals(List.of("222222222222", "111111111111"), provider.getAccountIds());
    }

    public void testGetAccountIdsRejectsInvalidAccountId() {
        PluginSettingSqsAccountProvider provider = new PluginSettingSqsAccountProvider();
        provider.initialize(Settings.builder().putList(TimeSeriesSettings.SQS_ACCOUNT_IDS.getKey(), "invalid-account").build());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, provider::getAccountIds);

        assertTrue(exception.getMessage().contains("Invalid AWS account ID"));
    }

    public void testGetAccountIdsRejectsEmptyConfiguration() {
        PluginSettingSqsAccountProvider provider = new PluginSettingSqsAccountProvider();

        provider.initialize(Settings.EMPTY);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, provider::getAccountIds);
        assertTrue(exception.getMessage().contains(TimeSeriesSettings.SQS_ACCOUNT_IDS.getKey()));
    }

    public void testFindRejectsUnknownProviderType() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> JobQueueAccountIdProvider.find("unknown_provider", Settings.EMPTY)
        );

        assertTrue(exception.getMessage().contains("No JobQueueAccountIdProvider found for type [unknown_provider]"));
    }
}
