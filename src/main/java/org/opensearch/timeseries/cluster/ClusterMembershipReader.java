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

package org.opensearch.timeseries.cluster;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.node.DiscoveryNodes.Builder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.TransportUtil;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * Optimized reader for the DynamoDB table written by CloudMapWatcher.
 *
 * This reader implements several key optimizations:
 * 1. Projection optimization: First queries only for revisionId to check if data has changed
 * 2. Conditional full read: Only fetches the complete item (including tasks) when revision changes
 * 3. Efficient latest query: Uses Query with reverse sort order and limit=1 to get latest revision
 * 4. Local caching: Maintains local revision state to avoid unnecessary reads
 */
public final class ClusterMembershipReader implements AutoCloseable, Runnable {

    private static final Logger LOG = LogManager.getLogger(ClusterMembershipReader.class);

    private static final String PK_COLUMN = "PK";
    private static final String REVISION_ID_COLUMN = "revisionId";
    private static final String TASKS_COLUMN = "tasks";
    private static final String SERVICE_PK_PREFIX = "service#";

    private final Region region;
    private final String service;
    private final String tableName;
    private final DynamoDbAsyncClient ddb;
    private final Clock clock;
    private final int opensearchPort;
    private final List<String> staticNodes;

    // Local state to track current revision
    private static final AtomicReference<DiscoveryNodes> SHARED_CACHED_DISCOVERY_NODES = new AtomicReference<>(
        DiscoveryNodes.builder().build()
    );
    private volatile long localRevisionId = -1L;
    private volatile DiscoveryNodes cachedDiscoveryNodes = SHARED_CACHED_DISCOVERY_NODES.get();
    private volatile long lastReadTimestamp = 0;
    private HashRing hashRing;

    /**
     * Represents the result of reading service revision data
     */
    public static class ServiceRevisionData {
        private final long revisionId;
        private final List<String> tasks;
        private final boolean isNewRevision;

        public ServiceRevisionData(long revisionId, boolean isNewRevision, List<String> tasks) {
            this.revisionId = revisionId;
            this.isNewRevision = isNewRevision;
            this.tasks = tasks;
        }

        public long getRevisionId() {
            return revisionId;
        }

        public List<String> getTasks() {
            return tasks;
        }

        public boolean isNewRevision() {
            return isNewRevision;
        }
    }

    public ClusterMembershipReader(Settings settings, Clock clock, HashRing hashRing) {
        this.clock = clock;
        this.hashRing = Objects.requireNonNull(hashRing, "hashRing cannot be null");
        this.staticNodes = TimeSeriesSettings.STATIC_NODES.get(settings);
        this.opensearchPort = TimeSeriesSettings.OPENSEARCH_PORT.get(settings);

        if (staticNodes.isEmpty()) {
            this.region = Region.of(TimeSeriesSettings.REGION.get(settings));
            this.service = TimeSeriesSettings.CLOUD_MAP_SERVICE.get(settings);
            this.tableName = TimeSeriesSettings.CLOUD_MAP_TABLE_NAME.get(settings);
            this.ddb = doPrivileged(
                () -> DynamoDbAsyncClient.builder().region(region).credentialsProvider(SecurityUtil.createCredentialsProvider()).build()
            );
        } else {
            this.region = null;
            this.service = null;
            this.tableName = null;
            this.ddb = null;
        }
    }

    public static DiscoveryNodes getCachedDiscoveryNodes() {
        return SHARED_CACHED_DISCOVERY_NODES.get();
    }

    /**
     * Reads the latest service revision data with optimizations.
     *
     * First performs a lightweight query to check if the revision has changed.
     * Only if the revision is different from local cache, performs a full read.
     *
     * @return CompletionStage containing ServiceRevisionData
     */
    public CompletionStage<ServiceRevisionData> readLatestRevision() {
        if (staticNodes.isEmpty() == false) {
            long staticRevisionId = staticNodes.hashCode();
            return CompletableFuture
                .completedFuture(new ServiceRevisionData(staticRevisionId, staticRevisionId != localRevisionId, staticNodes));
        }
        return readLatestRevisionIdOnly().thenCompose(this::conditionallyReadFullData).exceptionally(this::handleException);
    }

    /**
     * Query only for revisionId to check if data has changed.
     * Uses projection expression to minimize data transfer.
     */
    private CompletionStage<Long> readLatestRevisionIdOnly() {
        QueryRequest request = QueryRequest
            .builder()
            .tableName(tableName)
            .keyConditionExpression(PK_COLUMN + " = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(SERVICE_PK_PREFIX + service)))
            .projectionExpression(REVISION_ID_COLUMN)  // Only fetch revisionId
            .scanIndexForward(false)  // Descending order to get latest first
            .limit(1)  // Only need the latest
            .build();

        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                LOG.debug("No revision found for service: {}", service);
                return -1L;
            }

            AttributeValue revisionAttr = response.items().get(0).get(REVISION_ID_COLUMN);
            return revisionAttr != null ? Long.parseLong(revisionAttr.n()) : -1L;
        });
    }

    /**
     * Conditionally read full data if revision has changed.
     *
     * @param remoteRevisionId The latest revision ID from DynamoDB
     * @return ServiceRevisionData with appropriate caching behavior
     */
    private CompletionStage<ServiceRevisionData> conditionallyReadFullData(long remoteRevisionId) {
        // If revision hasn't changed, return cached data
        if (remoteRevisionId == localRevisionId && remoteRevisionId != -1L) {
            LOG.debug("Revision {} unchanged, using cached data with {} tasks", remoteRevisionId, cachedDiscoveryNodes.getSize());
            return CompletableFuture.completedFuture(new ServiceRevisionData(remoteRevisionId, false, List.of()));
        }

        // If no data exists remotely
        if (remoteRevisionId == -1L) {
            updateLocalState(-1L, List.of());
            return CompletableFuture.completedFuture(new ServiceRevisionData(-1L, localRevisionId != -1L, List.of()));
        }

        // Revision has changed, fetch full data
        return readFullRevisionData(remoteRevisionId);
    }

    /**
     * Read complete item including tasks list when revision has changed.
     */
    private CompletionStage<ServiceRevisionData> readFullRevisionData(long revisionId) {
        QueryRequest request = QueryRequest
            .builder()
            .tableName(tableName)
            .keyConditionExpression(PK_COLUMN + " = :pk AND " + REVISION_ID_COLUMN + " = :rev")
            .expressionAttributeValues(
                Map.of(":pk", AttributeValue.fromS(SERVICE_PK_PREFIX + service), ":rev", AttributeValue.fromN(String.valueOf(revisionId)))
            )
            .build();

        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                LOG.warn("Revision {} not found for service {}", revisionId, service);
                return new ServiceRevisionData(revisionId, true, List.of());
            }

            Map<String, AttributeValue> item = response.items().get(0);
            List<String> tasks = extractTasksFromItem(item);

            boolean isNewRevision = (revisionId != localRevisionId);

            LOG.info("Read revision {} with {} tasks (new: {})", revisionId, tasks.size(), isNewRevision);

            return new ServiceRevisionData(revisionId, isNewRevision, tasks);
        });
    }

    /**
     * Alternative method: Read latest data without revision checking.
     * Useful for initial bootstrap.
     */
    public CompletionStage<ServiceRevisionData> readLatestRevisionForce() {
        if (staticNodes.isEmpty() == false) {
            long staticRevisionId = staticNodes.hashCode();
            return CompletableFuture
                .completedFuture(new ServiceRevisionData(staticRevisionId, staticRevisionId != localRevisionId, staticNodes));
        }
        QueryRequest request = QueryRequest
            .builder()
            .tableName(tableName)
            .keyConditionExpression(PK_COLUMN + " = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(SERVICE_PK_PREFIX + service)))
            .scanIndexForward(false)  // Latest first
            .limit(1)
            .build();

        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                updateLocalState(-1L, List.of());
                return new ServiceRevisionData(-1L, true, List.of());
            }

            Map<String, AttributeValue> item = response.items().get(0);
            long revisionId = Long.parseLong(item.get(REVISION_ID_COLUMN).n());
            List<String> tasks = extractTasksFromItem(item);

            boolean isNewRevision = (revisionId != localRevisionId);

            return new ServiceRevisionData(revisionId, isNewRevision, tasks);
        }).exceptionally(this::handleException);
    }

    /**
     * Extract tasks list from DynamoDB item.
     */
    private List<String> extractTasksFromItem(Map<String, AttributeValue> item) {
        AttributeValue tasksAttr = item.get(TASKS_COLUMN);
        if (tasksAttr == null || tasksAttr.l() == null) {
            return List.of();
        }

        return tasksAttr.l().stream().map(AttributeValue::s).filter(s -> s != null && !s.isBlank()).toList();
    }

    /**
     * Update local state thread-safely.
     */
    private void updateLocalState(long revisionId, List<String> tasks) {
        // Create DiscoveryNodes.Delta based on the difference between cachedTasks and
        // input tasks
        DiscoveryNodes newDiscoveryNodes = buildDiscoveryNodesFromTasks(tasks);
        DiscoveryNodes.Delta delta = newDiscoveryNodes.delta(cachedDiscoveryNodes);

        try {
            if (delta.added() || delta.removed()) {
                LOG.info("Detected cluster membership changes - added: {}, removed: {}", delta.added(), delta.removed());

                // Call HashRing.buildCircles as we did in ClusterEventListener.clusterChanged
                hashRing.addNodeChangeEvent();
                hashRing.buildCircles(delta, ActionListener.wrap(hasRingBuildDone -> {
                    LOG.info("Hash ring build result from membership update: {}", hasRingBuildDone);
                    // Update local state
                    updateCachedDiscoveryNodes(newDiscoveryNodes);
                    this.localRevisionId = revisionId;
                    this.lastReadTimestamp = clock.millis();
                }, e -> { LOG.error("Failed updating hash ring from membership changes", e); }));
            } else {
                // No changes, update local state so we don't need to fetch the full data again and again
                updateCachedDiscoveryNodes(newDiscoveryNodes);
                this.localRevisionId = revisionId;
                this.lastReadTimestamp = clock.millis();
            }
        } catch (Exception e) {
            LOG.error("Failed to process cluster membership changes", e);
        }
    }

    private void updateCachedDiscoveryNodes(DiscoveryNodes newDiscoveryNodes) {
        cachedDiscoveryNodes = newDiscoveryNodes;
        SHARED_CACHED_DISCOVERY_NODES.set(newDiscoveryNodes);
    }

    /**
     * Create DiscoveryNodes based on tasks list.
     * Tasks are IP addresses, so we need to map them to DiscoveryNode objects.
     */
    private DiscoveryNodes buildDiscoveryNodesFromTasks(List<String> tasks) {
        Builder builder = DiscoveryNodes.builder();
        for (String task : tasks) {
            if (task == null || task.isBlank()) {
                continue;
            }
            var node = task.contains(":")
                ? TransportUtil.createDiscoveryNodeFromIpPort(task)
                : TransportUtil.createDiscoveryNodeFromIp(task, opensearchPort);
            if (node != null) {
                builder.add(node);
            }
        }

        return builder.build();
    }

    /**
     * Handle exceptions consistently.
     */
    private ServiceRevisionData handleException(Throwable ex) {
        if (ex instanceof ResourceNotFoundException) {
            LOG.error("DynamoDB table {} not found. Check CloudFormation deployment.", tableName);
        } else if (ex instanceof DynamoDbException) {
            LOG.warn("DynamoDB error reading service revisions", ex);
        } else {
            LOG.error("Unexpected error reading service revisions", ex);
        }

        // Return cached data if available, otherwise empty
        return new ServiceRevisionData(localRevisionId, false, List.of());
    }

    /**
     * Get current local state without querying DynamoDB.
     */
    public ServiceRevisionData getCachedData() {
        return new ServiceRevisionData(localRevisionId, false, List.of());
    }

    /**
     * Check if local cache is considered fresh (within last N minutes).
     */
    public boolean isCacheFresh(long maxAgeMs) {
        return (clock.millis() - lastReadTimestamp) < maxAgeMs;
    }

    /**
     * Reset local state (useful for testing or forced refresh scenarios).
     */
    public void resetLocalState() {
        updateLocalState(-1L, List.of());
    }

    @Override
    public void close() {
        if (ddb != null) {
            ddb.close();
        }
    }

    @Override
    public void run() {
        readLatestRevision().whenComplete((serviceRevisionData, throwable) -> {
            if (throwable != null) {
                LOG.error("Failed to read latest revision", throwable);
                return;
            }

            if (serviceRevisionData != null && serviceRevisionData.isNewRevision()) {
                updateLocalState(serviceRevisionData.getRevisionId(), serviceRevisionData.getTasks());
            }
        });
    }
}
