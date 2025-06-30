/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.sqs;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.opensearch.common.settings.Settings;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Reads the ordered list of SQS-owning AWS accounts from plugin settings.
 */
public class PluginSettingSqsAccountProvider implements JobQueueAccountIdProvider {

    public static final String PROVIDER_TYPE = "plugin_setting";
    private static final Pattern AWS_ACCOUNT_ID = Pattern.compile("\\d{12}");

    private volatile Settings settings;

    @Override
    public String getType() {
        return PROVIDER_TYPE;
    }

    @Override
    public void initialize(Settings settings) {
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    @Override
    public List<String> getAccountIds() {
        Settings initializedSettings = Objects.requireNonNull(settings, "Provider has not been initialized");
        List<String> ids = normalize(TimeSeriesSettings.SQS_ACCOUNT_IDS.get(initializedSettings));
        if (ids.isEmpty()) {
            throw new IllegalArgumentException(TimeSeriesSettings.SQS_ACCOUNT_IDS.getKey() + " must be defined");
        }
        return ids;
    }

    static List<String> normalize(List<String> rawAccountIds) {
        LinkedHashSet<String> normalized = rawAccountIds
            .stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(id -> !id.isEmpty())
            .collect(Collectors.toCollection(LinkedHashSet::new));

        for (String accountId : normalized) {
            if (!AWS_ACCOUNT_ID.matcher(accountId).matches()) {
                throw new IllegalArgumentException("Invalid AWS account ID [" + accountId + "]");
            }
        }

        return List.copyOf(normalized);
    }
}
