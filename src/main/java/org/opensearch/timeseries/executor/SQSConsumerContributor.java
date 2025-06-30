/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.executor;

import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/** SQS Consumer thread pool contributor */
public class SQSConsumerContributor implements ExecutorBuilderContributor {

    @Override
    public void contribute(Settings settings, List<ExecutorBuilder<?>> builders) {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE)) {
            return; // not my turn
        }

        builders
            .add(
                new FixedExecutorBuilder(
                    settings,
                    CommonName.SQS_CONSUMER_THREAD_POOL_NAME,
                    1,
                    1,
                    CommonName.SETTING_PREFIX + CommonName.SQS_CONSUMER_THREAD_POOL_NAME
                )
            );
    }
}
