/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.executor;

import java.util.List;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.timeseries.executor.ExecutorBuilderContributor;
import org.opensearch.timeseries.settings.DynamicStringSetting;

public class ADCoordinatorContributor implements ExecutorBuilderContributor {

    @Override
    public void contribute(Settings settings, List<ExecutorBuilder<?>> builders) {
        if (!DynamicStringSetting.getInstance().isCoordinatorNode()) {
            return;                                // not my turn
        }

        builders
            .add(
                new ScalingExecutorBuilder(
                    ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME,
                    // one for reading from cloudmap; another one for querying customer collections;
                    // another one for talking to model nodes
                    3,
                    // similar to netty in https://github.com/apache/uniffle/issues/1586,
                    // we use 2 * number of cores as the thread pool size because coordinator
                    // is IO bound.
                    Math.max(3, OpenSearchExecutors.allocatedProcessors(settings) * 2),
                    TimeValue.timeValueMinutes(10),
                    ADCommonName.AD_THREAD_POOL_PREFIX + ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME
                )
            );
    }

}
