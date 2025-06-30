/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;
import java.util.function.Consumer;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.commons.authuser.User;

/**
 * Run context for SDK-backed calls where credentials are resolved by the AWS SDK (task role).
 */
public class SdkRunContext implements RunContext {
    private static final RestorableContext NO_OP_CONTEXT = () -> {};

    public SdkRunContext() {}

    @Override
    public User getUser() {
        // TODO: plumb user context for SDK-backed flows if/when available
        return null;
    }

    /**
     * Executes the action using SDK-resolved task role credentials.
     * This method does not add any extra security context beyond what the SDK already provides.
     *
     * @param action the action to execute
     * @param onFailure the handler for any exception thrown by the action
     * @throws NullPointerException if action or onFailure is null
     */
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
        try {
            action.accept(NO_OP_CONTEXT);
        } catch (Exception exception) {
            onFailure.accept(exception);
        }
    }
}
