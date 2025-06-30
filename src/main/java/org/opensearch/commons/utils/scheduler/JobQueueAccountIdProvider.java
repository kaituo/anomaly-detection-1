/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.commons.utils.scheduler;

import java.util.List;
import java.util.ServiceLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;

/**
 * Provides AWS account IDs hosting SQS queues for polling.
 *
 * Implementations are discovered via {@link ServiceLoader}. Each implementation must:
 * <ul>
 * <li>Have a public no-arg constructor</li>
 * <li>Declare itself in {@code META-INF/services/org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider}</li>
 * <li>Return a unique {@link #getType()} string</li>
 * <li>Accept configuration via {@link #initialize(Settings)} before {@link #getAccountIds()} is called</li>
 * </ul>
 */
public interface JobQueueAccountIdProvider {

    /**
     * Identifier used to select this provider via configuration.
     */
    String getType();

    /**
     * Initialize the provider with node settings. Called once after discovery
     * and before {@link #getAccountIds()}.
     */
    void initialize(Settings settings);

    /**
     * Returns the current set of discovered AWS account IDs.
     */
    List<String> getAccountIds();

    /**
     * Find a provider matching {@code providerType}, initialize it with {@code settings},
     * and return it ready to use.
     */
    static JobQueueAccountIdProvider find(String providerType, Settings settings) {
        Logger log = LogManager.getLogger(JobQueueAccountIdProvider.class);
        ServiceLoader<JobQueueAccountIdProvider> loader = ServiceLoader
            .load(JobQueueAccountIdProvider.class, JobQueueAccountIdProvider.class.getClassLoader());

        for (JobQueueAccountIdProvider provider : loader) {
            String discoveredType = provider.getType();
            log.info("Discovered JobQueueAccountIdProvider: [{}]", discoveredType);
            if (providerType.equals(discoveredType)) {
                log.info("Found JobQueueAccountIdProvider for type [{}]", providerType);
                provider.initialize(settings);
                return provider;
            }
        }
        throw new IllegalArgumentException("No JobQueueAccountIdProvider found for type [" + providerType + "]");
    }
}
