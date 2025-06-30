/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.function.Consumer;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.commons.authuser.User;

/**
 * Executes actions with system-level authentication.
 */
public interface RunContext {
    interface RestorableContext {
        void restore();
    }

    void runWithSystemAuth(CheckedRunnable<Exception> action, Consumer<Exception> onFailure);

    void runWithSystemAuth(CheckedConsumer<RestorableContext, Exception> action, Consumer<Exception> onFailure);

    /**
     * Returns the current user from the active thread context, or null when not available.
     */
    User getUser();
}
