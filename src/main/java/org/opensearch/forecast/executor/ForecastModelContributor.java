/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.executor;

import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.timeseries.executor.ExecutorBuilderContributor;

public class ForecastModelContributor implements ExecutorBuilderContributor {

    @Override
    public void contribute(Settings settings, List<ExecutorBuilder<?>> builders) {
        builders
            .add(
                new ScalingExecutorBuilder(
                    ForecastCommonName.FORECAST_THREAD_POOL_NAME,
                    1,
                    // this pool is used by both real time and run once.
                    // HCAD can be heavy after supporting 1 million entities.
                    // Limit to use at most 3/4 of the processors.
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) * 3 / 4),
                    TimeValue.timeValueMinutes(10),
                    ForecastCommonName.FORECAST_THREAD_POOL_PREFIX + ForecastCommonName.FORECAST_THREAD_POOL_NAME
                )
            );

    }

}
