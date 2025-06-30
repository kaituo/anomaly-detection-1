/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.timeseries.util;

import static org.opensearch.timeseries.constant.CommonName.TENANT_ID_HEADER;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;

/**
 * Helper class for tenant ID validation and throttling in time series analytics
 */
public class TenantAwareHelper {

    private TenantAwareHelper() {}

    /**
     * Finds the tenant id in the REST Headers.
     * <p>
     * The tenant id is the raw {@code x-tenant-id} header value. In multi-tenant mode it must be
     * supplied by the caller in {@code accountId:applicationId:workspaceId} format.
     *
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @param restRequest the RestRequest
     * @return The tenant ID from the headers or null if multitenancy is not enabled
     */
    public static String getTenantID(Boolean isMultiTenancyEnabled, RestRequest restRequest) {
        if (!isMultiTenancyEnabled) {
            return null;
        }

        Map<String, List<String>> headers = restRequest.getHeaders();

        String tenantId = firstHeaderValue(headers, TENANT_ID_HEADER);
        if (tenantId == null) {
            throw new OpenSearchStatusException("Tenant ID header is missing or has no value", RestStatus.FORBIDDEN);
        }

        return tenantId;
    }

    private static String firstHeaderValue(Map<String, List<String>> headers, String headerName) {
        List<String> values = headers.get(headerName);
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    /**
     * Validates that tenant ID is not null/blank when multi-tenancy is enabled.
     * Call this method at entry points (transport actions, handlers) to fail early.
     *
     * @param tenantId the tenant ID to validate
     * @param settings the settings instance
     * @param multiTenancyEnabledSetting the setting that indicates if multi-tenancy is enabled
     * @throws OpenSearchStatusException if multi-tenancy is enabled but tenant ID is null/blank
     */
    public static void validateTenantId(String tenantId, Settings settings, Setting<Boolean> multiTenancyEnabledSetting) {
        if (multiTenancyEnabledSetting.get(settings) && Strings.isBlank(tenantId)) {
            throw new OpenSearchStatusException("Tenant ID is required in multi-tenancy mode", RestStatus.BAD_REQUEST);
        }
    }

    /**
     * Validates that tenant ID is not null/blank when multi-tenancy is enabled.
     * Overload that takes a boolean directly.
     *
     * @param tenantId the tenant ID to validate
     * @param isMultiTenancyEnabled whether multi-tenancy is enabled
     * @throws OpenSearchStatusException if multi-tenancy is enabled but tenant ID is null/blank
     */
    public static void validateTenantId(String tenantId, boolean isMultiTenancyEnabled) {
        if (isMultiTenancyEnabled && Strings.isBlank(tenantId)) {
            throw new OpenSearchStatusException("Tenant ID is required in multi-tenancy mode", RestStatus.BAD_REQUEST);
        }
    }

    /**
     * Parses a tenant id in {@code accountId:applicationId:workspaceId} format.
     *
     * @param tenantId tenant id to parse
     * @return tenant id components
     */
    public static TenantComponents parseTenantId(String tenantId) {
        if (tenantId == null) {
            throw new IllegalArgumentException("Tenant id cannot be null");
        }

        String[] parts = tenantId.split(":", -1);
        if (parts.length != 3 || Strings.isBlank(parts[0]) || Strings.isBlank(parts[1]) || Strings.isBlank(parts[2])) {
            throw new IllegalArgumentException("Invalid tenant id format: " + tenantId);
        }

        return new TenantComponents(parts[0], parts[1], parts[2]);
    }

    /**
     * Components of the raw AOSD tenant id header.
     */
    public record TenantComponents(String accountId, String applicationId, String workspaceId) {
    }

    /**
     * Resolves tenant IDs supplied via header and request body.
     * Body tenant ID is optional, but if present it must match the header value.
     *
     * @param headerTenantId tenant ID supplied in request header
     * @param bodyTenantId tenant ID supplied in request body
     * @return resolved tenant ID
     */
    public static String reconcileTenantId(String headerTenantId, String bodyTenantId) {
        if (Strings.isBlank(bodyTenantId)) {
            return headerTenantId;
        }
        if (Strings.isBlank(headerTenantId)) {
            return bodyTenantId;
        }
        if (headerTenantId.equals(bodyTenantId) == false) {
            throw new OpenSearchStatusException("Tenant ID in header and body must match", RestStatus.BAD_REQUEST);
        }
        return headerTenantId;
    }
}
