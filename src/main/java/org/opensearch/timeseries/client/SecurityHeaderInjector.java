/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.common.Strings;

/**
 * A helper class to inject security headers into the http request.
 * 
 * Usage guide:
 * - On the call sites, wrap instances in try-with-resources so {@link #close()} always runs;
 * it restores prior headers or removes the ThreadLocal value, preventing leaks.
 * - The ThreadLocal object itself is a field on caller, but after {@link #close()} runs it holds no
 *   per-thread data.
 */
public class SecurityHeaderInjector implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SecurityHeaderInjector.class);
    private final String id;
    private final Map<String, String> previousHeaders;
    private final ThreadLocal<Map<String, String>> securityHeaders;
    private final Settings settings;

    public SecurityHeaderInjector(String id, Settings settings, ThreadLocal<Map<String, String>> securityHeaders) {
        this.id = id;
        this.securityHeaders = Objects.requireNonNull(securityHeaders, "securityHeaders must not be null");
        this.previousHeaders = securityHeaders.get();
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    public void inject(String user, List<String> roles) {
        boolean injectUser = settings.getAsBoolean(ConfigConstants.OPENSEARCH_SECURITY_USE_INJECTED_USER_FOR_PLUGINS, false);
        if (injectUser) {
            injectUser(user);
        } else {
            injectRoles(roles);
        }
    }

    private void injectUser(String user) {
        if (Strings.isNullOrEmpty(user)) {
            return;
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(ConfigConstants.INJECTED_USER, user);
        securityHeaders.set(headers);
        LOG.debug("Injected user header for {}", id);
    }

    private void injectRoles(List<String> roles) {
        if (roles == null || roles.isEmpty()) {
            return;
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES, "plugin|" + String.join(",", roles));
        securityHeaders.set(headers);
        LOG.debug("Injected role headers for {}", id);
    }

    @Override
    public void close() {
        if (previousHeaders != null) {
            securityHeaders.set(previousHeaders);
        } else {
            securityHeaders.remove();
        }
    }
}
