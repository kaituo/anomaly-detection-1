/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.timeseries.annotation.SuppressForbidden;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ThreadRunContext implements RunContext {
    private final ThreadContext threadContext;

    /**
     * Holds the shared {@link ThreadContext} instance. {@link ThreadContext} stores per-thread
     * state in a {@code ThreadLocal} (e.g., {@code private final ThreadLocal<ThreadContextStruct> threadLocal;}),
     * so different request-handling threads see different user/security headers. Callers should not
     * capture a {@link ThreadContext.StoredContext} at node creation time; instead, stash/restore at
     * execution time (as this class does) to use the current thread's context.
     * <p>
     * The {@link org.opensearch.transport.client.Client} is typically a singleton per node and does
     * not carry user identity itself; it reads the active {@link ThreadContext} when executing
     * requests. This means the same client can safely serve different users on different threads.
     */
    public ThreadRunContext(ThreadContext threadContext) {
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext must not be null");
    }

    /**
     * Generates a user string formed by the username, backend roles, roles and requested tenants separated by '|'
     * (e.g., john||own_index,testrole|__user__, no backend role so you see two verticle line after john.).
     * This is the user string format used internally in the OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT and may be
     * parsed using User.parse(string).
     * @param client Client containing user info. A public API request will fill in the user info in the thread context.
     * @return parsed user object
     */
    /**
     * @deprecated Prefer {@link org.opensearch.timeseries.client.RunContext#getUser()} so callers
     * can use the appropriate RunContext implementation.
     */
    @Override
    public User getUser() {
        String userStr = threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        return userStr == null ? null : User.parse(userStr);
    }

    @Override
    public void runWithSystemAuth(CheckedRunnable<Exception> action, Consumer<Exception> onFailure) {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(onFailure, "onFailure must not be null");
        runWithSystemAuth(context -> action.run(), onFailure);
    }

    @Override
    public void runWithSystemAuth(CheckedConsumer<RestorableContext, Exception> action, Consumer<Exception> onFailure) {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(onFailure, "onFailure must not be null");
        try (StoredContextWrapper context = new StoredContextWrapper(threadContext.stashContext())) {
            action.accept(context);
        } catch (Exception exception) {
            onFailure.accept(exception);
        }
    }

    /**
     * Idempotent wrapper around {@link ThreadContext.StoredContext} that allows call sites to
     * "restore early" via {@link #restore()} while the outer try-with-resources can still safely
     * invoke {@link #close()} without double-closing the underlying stored context.
     */
    private static final class StoredContextWrapper implements RestorableContext, AutoCloseable {
        private final ThreadContext.StoredContext storedContext;
        private final AtomicBoolean restored = new AtomicBoolean(false);

        private StoredContextWrapper(ThreadContext.StoredContext storedContext) {
            this.storedContext = storedContext;
        }

        @Override
        public void restore() {
            if (restored.compareAndSet(false, true)) {
                storedContext.close();
            }
        }

        @Override
        public void close() {
            restore();
        }
    }
}
