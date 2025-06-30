/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * ThreadContext helpers for selecting the current data-plane client strategy.
 */
public final class DataPlaneClientFactoryContext {
    public static final String SQS_SIGNING_FACTORY_KEY = "timeseries.dataplane.sqs.signing.factory";
    private static final ThreadLocal<DataPlaneClientFactory.RequestContext> CURRENT_REQUEST_CONTEXT = new ThreadLocal<>();

    private DataPlaneClientFactoryContext() {}

    public static void setCurrentFactory(ThreadContext threadContext, DataPlaneClientFactory factory) {
        if (factory != null) {
            threadContext.putTransient(SQS_SIGNING_FACTORY_KEY, factory);
        }
    }

    public static DataPlaneClientFactory getCurrentFactory(ThreadContext threadContext) {
        return threadContext.getTransient(SQS_SIGNING_FACTORY_KEY);
    }

    public static void setCurrentRequestContext(DataPlaneClientFactory.RequestContext requestContext) {
        if (requestContext == null) {
            CURRENT_REQUEST_CONTEXT.remove();
        } else {
            CURRENT_REQUEST_CONTEXT.set(requestContext);
        }
    }

    public static DataPlaneClientFactory.RequestContext getCurrentRequestContext() {
        return CURRENT_REQUEST_CONTEXT.get();
    }

    public static <T> BiConsumer<T, Throwable> preserveCurrentRequestContext(BiConsumer<T, Throwable> callback) {
        DataPlaneClientFactory.RequestContext requestContext = getCurrentRequestContext();
        return (response, throwable) -> runWithRequestContext(requestContext, () -> callback.accept(response, throwable));
    }

    public static void runWithRestoredContext(Supplier<ThreadContext.StoredContext> restorableContext, Runnable runnable) {
        if (restorableContext == null) {
            runnable.run();
            return;
        }
        try (ThreadContext.StoredContext ignored = restorableContext.get()) {
            runnable.run();
        }
    }

    /**
     * Run under the captured {@link DataPlaneClientFactory.RequestContext}, restoring the previous context afterward.
     *
     * <p>
     * This temporarily puts the captured {@code requestContext} into {@link DataPlaneClientFactoryContext}, invokes the callback,
     * then restores whatever context was there before:
     * </p>
     *
     * <pre>{@code
     * public static void runWithRequestContext(DataPlaneClientFactory.RequestContext requestContext, Runnable runnable) {
     *     DataPlaneClientFactory.RequestContext previous = getCurrentRequestContext();
     *     setCurrentRequestContext(requestContext);
     *     try {
     *         runnable.run();
     *     } finally {
     *         setCurrentRequestContext(previous);
     *     }
     * }
     * }</pre>
     *
     * <p>
     * Async callbacks use the captured context while dispatching to the caller's listener and still close request preparation
     * afterward:
     * </p>
     *
     * <pre>{@code
     * @Override
     * public void onSuccess(Response response) {
     *     try {
     *         DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> responseListener.onSuccess(response));
     *     } finally {
     *         requestPreparation.close();
     *     }
     * }
     *
     * @Override
     * public void onFailure(Exception e) {
     *     try {
     *         DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> responseListener.onFailure(e));
     *     } finally {
     *         requestPreparation.close();
     *     }
     * }
     * }</pre>
     *
     * <p>
     * The chained AOSS paths also reuse the captured context explicitly. One {@code requestContext} is created before search,
     * passed to search, and then passed to the follow-up update/delete calls:
     * </p>
     *
     * <pre>{@code
     * DataPlaneClientFactory.RequestContext requestContext = getDataPlaneRequestContext(tenantContext);
     * executeRestSearch(request.getSearchRequest(), tenantContext, requestContext, ...);
     *
     * executeRestUpdate(restUpdateRequest, requestContext, ...);
     * executeRestDelete(restDeleteRequest, requestContext, ...);
     * }</pre>
     */
    public static void runWithRequestContext(DataPlaneClientFactory.RequestContext requestContext, Runnable runnable) {
        DataPlaneClientFactory.RequestContext previous = getCurrentRequestContext();
        setCurrentRequestContext(requestContext);
        try {
            runnable.run();
        } finally {
            setCurrentRequestContext(previous);
        }
    }
}
