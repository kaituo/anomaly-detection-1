/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.util;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.constant.CommonName;

@SuppressForbidden(reason = "org.opensearch.cluster.service.ClusterService#state usage: Only meant to be used in single-tenant.")
public class IndexUtils implements IndexOperations {
    public static final String ALIAS_POINTS_TO_MULTIPLE_INDICES_STATUS = "alias exists, but does not point to any " + "indices";

    private static final Logger logger = LogManager.getLogger(IndexUtils.class);

    private ClusterService clusterService;

    /**
     * Inject annotation required by Guice to instantiate EntityResultTransportAction (transitive dependency)
     *
     * @param clusterService ES ClusterService
     */
    @Inject
    public IndexUtils(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getIndexHealthStatus(String indexOrAliasName) throws IllegalArgumentException {
        if (!clusterService.state().getRoutingTable().hasIndex(indexOrAliasName)) {
            // Check if the index is actually an alias
            if (clusterService.state().metadata().hasAlias(indexOrAliasName)) {
                // List of all indices the alias refers to
                List<IndexMetadata> indexMetaDataList = clusterService
                    .state()
                    .metadata()
                    .getIndicesLookup()
                    .get(indexOrAliasName)
                    .getIndices();
                if (indexMetaDataList.size() == 0) {
                    return ALIAS_EXISTS_NO_INDICES_STATUS;
                } else if (indexMetaDataList.size() > 1) {
                    throw new IllegalArgumentException("Cannot get health for alias that points to multiple indices");
                } else {
                    indexOrAliasName = indexMetaDataList.get(0).getIndex().getName();
                }
            } else {
                return NONEXISTENT_INDEX_STATUS;
            }
        }

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(
            clusterService.state().metadata().index(indexOrAliasName),
            clusterService.state().getRoutingTable().index(indexOrAliasName)
        );

        return indexHealth.getStatus().name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String resolveIndexName(String tenantId, String configId, String modelId, String defaultIndexName) {
        // single-tenant: use checkpoint index name as is
        return defaultIndexName;
    }

    /**
     * Recursively checks if schema1 is a superset of schema2.
     * This is used to validate that an actual index mapping contains all required fields
     * from the expected mapping schema.
     *
     * @param schema1 the potential superset schema object (actual mapping)
     * @param schema2 the subset schema object (expected mapping)
     * @return true if schema1 contains all fields and nested structures from schema2
     */
    public static boolean isSchemaSuperset(Object schema1, Object schema2) {
        if (schema1 == schema2) {
            return true;
        }
        if (schema1 == null || schema2 == null) {
            return false;
        }
        if (schema1 instanceof Map && schema2 instanceof Map) {
            Map<?, ?> map1 = (Map<?, ?>) schema1;
            Map<?, ?> map2 = (Map<?, ?>) schema2;
            for (Map.Entry<?, ?> entry : map2.entrySet()) {
                Object key = entry.getKey();
                if (!map1.containsKey(key)) {
                    return false;
                }
                if (!isSchemaSuperset(map1.get(key), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return schema1.equals(schema2);
    }

    /**
     * Parses result mapping JSON string and extracts the field configurations.
     *
     * @param resultMapping the JSON mapping string
     * @param logger logger for error reporting
     * @return Map of field configurations, or null if parsing fails
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    public static Map<String, Object> parseResultFieldConfigs(String resultMapping, Logger logger) {
        try {
            Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
            Object properties = asMap.get(CommonName.PROPERTIES);
            if (properties instanceof Map) {
                return (Map<String, Object>) properties;
            } else {
                logger.error("Fail to read result mapping file.");
            }
        } catch (Exception e) {
            logger.error("Fail to init result mapping", e);
        }
        return null;
    }

    /**
     * Validates that an actual mapping contains all required fields from the expected field configs.
     *
     * @param actualMapping the actual index mapping properties
     * @param expectedFieldConfigs the expected field configurations
     * @param logger logger for warning messages
     * @return true if the actual mapping is valid (contains all expected fields with correct schema)
     */
    public static boolean validateMappingFields(
        Map<String, Object> actualMapping,
        Map<String, Object> expectedFieldConfigs,
        Logger logger
    ) {
        for (String fieldName : expectedFieldConfigs.keySet()) {
            Object defaultSchema = expectedFieldConfigs.get(fieldName);
            if (!actualMapping.containsKey(fieldName)) {
                logger.warn("mapping mismatch due to missing {}", fieldName);
                return false;
            }
            Object actualSchema = actualMapping.get(fieldName);
            if (!isSchemaSuperset(actualSchema, defaultSchema)) {
                logger.warn("mapping mismatch due to {}", fieldName);
                return false;
            }
        }
        return true;
    }

    /**
     * Check if REST search should be used for the given indices.
     * This returns false if any of the indices are system indices (starting with ".").
     * We cannot use SystemIndices.isSystemIndex because it is not internal to OpenSearch core.
     *
     * Read: https://tinyurl.com/2z3n9vb8
     * @param indices array of index names
     * @return true if REST search should be used, false otherwise
     */
    public static boolean shouldUseRestSearch(String... indices) {
        return indices != null && indices.length > 0 && Arrays.stream(indices).noneMatch(index -> index.startsWith("."));
    }
}
