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

public class ADModelContributor implements ExecutorBuilderContributor {

    private static ScalingExecutorBuilder scalingPool(Settings settings, String name, int minThreads, int maxThreads) {

        return new ScalingExecutorBuilder(
            name,
            minThreads,
            maxThreads,
            TimeValue.timeValueMinutes(10),
            ADCommonName.AD_THREAD_POOL_PREFIX + name
        );
    }

    @Override
    public void contribute(Settings settings, List<ExecutorBuilder<?>> builders) {
        // when isCoordinatorNode() or isMasterNode() is true, we are in microservice mode.
        // otherwise, we are either the model node or not in microservice
        if (DynamicStringSetting.getInstance().isCoordinatorNode() || DynamicStringSetting.getInstance().isCloudmapWatcherNode()) {
            return;
        }

        int cores = OpenSearchExecutors.allocatedProcessors(settings);
        int halfCores = Math.max(1, cores / 2);
        int oneX = Math.max(1, cores);
        int twoX = Math.max(1, cores * 2);
        int eighthCores = Math.max(1, cores / 8);

        if (DynamicStringSetting.getInstance().isModelNode()) {
            builders
                .addAll(
                    List
                        .of(
                            // similar to netty in https://github.com/apache/uniffle/issues/1586,
                            // we use 2 * number of cores as the thread pool size because coordinator
                            // has mixed workloads.
                            scalingPool(settings, ADCommonName.AD_THREAD_POOL_NAME, 1, twoX),
                            // historical is CPU-bound
                            // Running more active threads than vCPUs forces the kernel to time-slice,
                            // adding context-switch overhead and cache thrashing.
                            scalingPool(settings, ADCommonName.AD_BATCH_TASK_THREAD_POOL_NAME, 1, oneX)
                        )
                );
        } else {
            builders
                .addAll(
                    List
                        .of(
                            scalingPool(settings, ADCommonName.AD_THREAD_POOL_NAME, 1, halfCores),
                            scalingPool(settings, ADCommonName.AD_BATCH_TASK_THREAD_POOL_NAME, 1, eighthCores)
                        )
                );
        }
    }

}
