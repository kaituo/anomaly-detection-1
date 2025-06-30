/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * ThreadContext helpers for selecting the current data-plane client strategy.
 */
public final class DataPlaneClientFactoryContext {
    public static final String SQS_SIGNING_FACTORY_KEY = "timeseries.dataplane.sqs.signing.factory";

    private DataPlaneClientFactoryContext() {}

    public static void setCurrentFactory(ThreadContext threadContext, DataPlaneClientFactory factory) {
        if (factory != null) {
            threadContext.putTransient(SQS_SIGNING_FACTORY_KEY, factory);
        }
    }

    public static DataPlaneClientFactory getCurrentFactory(ThreadContext threadContext) {
        return threadContext.getTransient(SQS_SIGNING_FACTORY_KEY);
    }
}
