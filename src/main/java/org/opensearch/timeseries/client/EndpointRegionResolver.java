/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.net.URI;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves AWS regions embedded in customer data-plane endpoints.
 */
final class EndpointRegionResolver {
    private static final Pattern SCHEME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.*");
    private static final Pattern AWS_REGION_PATTERN = Pattern.compile("(?<![a-z0-9])([a-z]{2}(?:-[a-z]+)+-\\d)(?![a-z0-9])");

    private EndpointRegionResolver() {}

    static String resolveRegionFromEndpointOrDefault(String endpoint, String defaultRegion) {
        return extractRegion(endpoint).orElse(defaultRegion);
    }

    static Optional<String> extractRegion(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return Optional.empty();
        }

        String host = endpoint.trim();
        try {
            URI uri = URI.create(SCHEME_PATTERN.matcher(host).matches() ? host : "https://" + host);
            if (uri.getHost() != null && uri.getHost().isBlank() == false) {
                host = uri.getHost();
            }
        } catch (IllegalArgumentException e) {
            // Fall back to scanning the original value. Endpoint validation happens when the RestClient is built.
        }

        Matcher matcher = AWS_REGION_PATTERN.matcher(host.toLowerCase(Locale.ROOT));
        String region = null;
        while (matcher.find()) {
            region = matcher.group(1);
        }
        return Optional.ofNullable(region);
    }
}
