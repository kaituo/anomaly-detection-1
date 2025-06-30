/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.executor;

import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;

public interface ExecutorBuilderContributor {
    /**
     * Add the pools that this contributor is responsible for.
     * The contributor decides at runtime whether it is applicable.
     */
    void contribute(Settings settings, List<ExecutorBuilder<?>> builders);
}
