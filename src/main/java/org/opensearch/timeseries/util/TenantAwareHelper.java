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
     * Finds the tenant id in the REST Headers
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @param restRequest the RestRequest
     * @return The tenant ID from the headers or null if multitenancy is not enabled
     */
    public static String getTenantID(Boolean isMultiTenancyEnabled, RestRequest restRequest) {
        if (!isMultiTenancyEnabled) {
            return null;
        }

        Map<String, List<String>> headers = restRequest.getHeaders();

        List<String> tenantIdList = headers.get(TENANT_ID_HEADER);
        if (tenantIdList == null || tenantIdList.isEmpty()) {
            throw new OpenSearchStatusException("Tenant ID header is missing or has no value", RestStatus.FORBIDDEN);
        }

        String tenantId = tenantIdList.get(0);
        if (tenantId == null) {
            throw new OpenSearchStatusException("Tenant ID can't be null", RestStatus.FORBIDDEN);
        }

        return tenantId;
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
}
