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

import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.Strings;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.StringUtil;

public class IndexUtils {
    /**
     * Status string of index that does not exist
     */
    public static final String NONEXISTENT_INDEX_STATUS = "non-existent";

    /**
     * Status string when an alias exists, but does not point to an index
     */
    public static final String ALIAS_EXISTS_NO_INDICES_STATUS = "alias exists, but does not point to any indices";
    public static final String ALIAS_POINTS_TO_MULTIPLE_INDICES_STATUS = "alias exists, but does not point to any " + "indices";

    /**
     * Constants for entity model key generation
     */
    public static final String ENTITY_MODEL_ID_INFIX = "_entity_";
    public static final int ENTITY_KEY_SEGMENT_LENGTH = 2;
    public static final int ENTITY_KEY_SEGMENT_DEPTH = 3;

    private static final Logger logger = LogManager.getLogger(IndexUtils.class);

    private ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Inject annotation required by Guice to instantiate EntityResultTransportAction (transitive dependency)
     *
     * @param clusterService ES ClusterService
     * @param indexNameExpressionResolver index name resolver
     */
    @Inject
    public IndexUtils(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /**
     * Gets the cluster index health for a particular index or the index an alias points to
     *
     * If an alias is passed in, it will only return the health status of an index it points to if it only points to a
     * single index. If it points to multiple indices, it will throw an exception.
     *
     * @param indexOrAliasName String of the index or alias name to get health of.
     * @return String represents the status of the index: "red", "yellow" or "green"
     * @throws IllegalArgumentException Thrown when an alias is passed in that points to more than one index
     */
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

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    public boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    /**
     * Resolve the dynamic index name that will eventually be used as the S3 key prefix.
     * S3CheckpointDao concatenates this value with the request id to form the final object key.
     * <p>
     * S3 parallelization best practice recommends creating many prefixes to spread traffic across partitions.
     * We therefore build a hierarchy of {@code <sanitized-tenant>/<config-id>/<detector-id>/<entity-chunks...>} so that
     * workload is distributed across both detectors and entities. Detector ids and entity ids are auto-generated
     * base64 strings, which already ensure good character distribution for the S3 prefix space.
     *
     * @param tenantId tenant id
     * @param configId config id
     * @param modelId model id whose id provides the entity-level prefixes
     * @param defaultIndexName fallback index name when tenant/config info is not available
     * @return prefix that will be combined with the request id to produce the S3 object key
     */
    public static <RCFModelType> String resolveIndexName(String tenantId, String configId, String modelId, String defaultIndexName) {
        // single-tenant: use checkpoint index name as is
        // multi-tenant: repurpose checkpoint index name as s3 key prefix
        if (Strings.isEmpty(tenantId) || Strings.isEmpty(configId)) {
            return defaultIndexName;
        }

        String basePrefix = StringUtil.sanitizeId(tenantId) + "/" + configId;
        String entityId = extractEntityIdentifier(modelId);

        if (Strings.isEmpty(entityId)) {
            // Single-stream detector: no entity suffix, so use tenant/config prefix as the full key.
            return basePrefix;
        }

        return buildEntityModelKey(basePrefix, entityId);
    }

    /**
     * Append short chunks of the entity identifier to the base prefix to finish building the S3 object key.
     * Example (detector id {@code ZoNYVJsq5ry6e-SWXmAt1Q}, entity id {@code _cLQbZUBxkwQb14jsXV9}):
     * {@code .../ZoNYVJsq5ry6e-SWXmAt1Q/_c/LQ/bZ/UBxkwQb14jsXV9}.
     * Splitting the entity id into fixed-length segments creates multiple sub-prefixes so that S3 can route
     * each prefix independently, avoiding hot-spotting within a single detector prefix.
     *
     * @param basePrefix hierarchy that already includes tenant, config, and detector prefixes
     * @param entityId full entity identifier, usually a base64 hash
     * @return S3 object key prefix with entity sub-prefixes appended
     */
    public static String buildEntityModelKey(String basePrefix, String entityId) {
        StringBuilder keyBuilder = new StringBuilder(basePrefix);

        int index = 0;
        int segmentCount = 0;
        while (segmentCount < ENTITY_KEY_SEGMENT_DEPTH && (index + ENTITY_KEY_SEGMENT_LENGTH) < entityId.length()) {
            // Break the entity id into short prefixes to distribute objects across partitions.
            keyBuilder.append("/").append(entityId, index, index + ENTITY_KEY_SEGMENT_LENGTH);
            index += ENTITY_KEY_SEGMENT_LENGTH;
            segmentCount++;
        }

        if (index < entityId.length()) {
            keyBuilder.append("/").append(entityId.substring(index));
        }

        return keyBuilder.toString();
    }

    /**
     * Extract entity identifier from model ID.
     * @param modelId the model ID containing entity information
     * @return entity identifier or empty string if not found
     */
    public static String extractEntityIdentifier(String modelId) {
        if (Strings.isEmpty(modelId)) {
            return "";
        }
        int entityInfixIndex = modelId.indexOf(ENTITY_MODEL_ID_INFIX);
        if (entityInfixIndex < 0) {
            return "";
        }
        int entityStartIndex = entityInfixIndex + ENTITY_MODEL_ID_INFIX.length();
        return entityStartIndex < modelId.length() ? modelId.substring(entityStartIndex) : "";
    }

}
