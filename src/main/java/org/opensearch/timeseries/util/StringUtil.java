package org.opensearch.timeseries.util;

import org.opensearch.core.common.Strings;

public class StringUtil {
    public static final String TENANT_CONFIG_SEPARATOR = "#";

    /**
     * Sanitize an id by replacing all non-alphanumeric characters (e.g., ":") with an underscore.
     * @param id the id to sanitize
     * @return the sanitized id
     */
    public static String sanitizeId(String id) {
        if (id == null) {
            return "";
        }
        return id.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    /**
     * Create a composite key for maps from tenant and config ID.
     *
     * @param tenantId tenant id
     * @param configId config id
     * @return composite key
     */
    public static String getCompositeKey(String tenantId, String configId) {
        if (Strings.isEmpty(tenantId)) {
            return configId;
        }
        return tenantId + TENANT_CONFIG_SEPARATOR + configId;
    }

    /**
     * Parse tenant id from a composite key
     *
     * @param key composite key
     * @return tenant id
     */
    public static String getTenantId(String key) {
        if (false == key.contains(TENANT_CONFIG_SEPARATOR)) {
            return "";
        }
        return key.split(TENANT_CONFIG_SEPARATOR)[0];
    }

    /**
     * Parse config id from a composite key
     *
     * @param key composite key
     * @return config id
     */
    public static String getConfigId(String key) {
        if (false == key.contains(TENANT_CONFIG_SEPARATOR)) {
            return key;
        }
        return key.split(TENANT_CONFIG_SEPARATOR)[1];
    }

    
    
}
