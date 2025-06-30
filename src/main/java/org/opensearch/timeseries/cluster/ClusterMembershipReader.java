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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.util.SecurityUtil;

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
public final class ClusterMembershipReader implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(ClusterMembershipReader.class);
    
    private final Region region;
    private final String service;
    private final String tableName;
    private final DynamoDbAsyncClient ddb;
    private final Clock clock;
    
    // Local state to track current revision
    private volatile long localRevisionId = -1L;
    private volatile List<String> cachedTasks = List.of();
    private volatile long lastReadTimestamp = 0;
    private HashRing hashRing;

    /**
     * Represents the result of reading service revision data
     */
    public static class ServiceRevisionData {
        private final long revisionId;
        private final List<String> tasks;
        private final boolean isNewRevision;
        
        public ServiceRevisionData(long revisionId, List<String> tasks, boolean isNewRevision) {
            this.revisionId = revisionId;
            this.tasks = tasks;
            this.isNewRevision = isNewRevision;
        }
        
        public long getRevisionId() { return revisionId; }
        public List<String> getTasks() { return tasks; }
        public boolean isNewRevision() { return isNewRevision; }
    }

    public ClusterMembershipReader(Settings settings, Clock clock, HashRing hashRing) {
        this.region = Region.of(AnomalyDetectorSettings.REGION.get(settings));
        this.service = AnomalyDetectorSettings.CLOUD_MAP_SERVICE.get(settings);
        this.tableName = AnomalyDetectorSettings.CLOUD_MAP_TABLE_NAME.get(settings);
        this.clock = clock;
        
        this.ddb = doPrivileged(
            () -> DynamoDbAsyncClient.builder()
                .region(region)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        );

        this.hashRing = hashRing;
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
        return readLatestRevisionIdOnly()
            .thenCompose(this::conditionallyReadFullData)
            .exceptionally(this::handleException);
    }

    /**
     * Query only for revisionId to check if data has changed.
     * Uses projection expression to minimize data transfer.
     */
    private CompletionStage<Long> readLatestRevisionIdOnly() {
        QueryRequest request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("PK = :pk")
            .expressionAttributeValues(Map.of(
                ":pk", AttributeValue.fromS("service#" + service)
            ))
            .projectionExpression("revisionId")  // Only fetch revisionId
            .scanIndexForward(false)  // Descending order to get latest first
            .limit(1)  // Only need the latest
            .build();
            
        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                LOG.debug("No revision found for service: {}", service);
                return -1L;
            }
            
            AttributeValue revisionAttr = response.items().get(0).get("revisionId");
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
            LOG.debug("Revision {} unchanged, using cached data with {} tasks", 
                remoteRevisionId, cachedTasks.size());
            return CompletableFuture.completedFuture(
                new ServiceRevisionData(remoteRevisionId, cachedTasks, false)
            );
        }
        
        // If no data exists remotely
        if (remoteRevisionId == -1L) {
            updateLocalState(-1L, List.of());
            return CompletableFuture.completedFuture(
                new ServiceRevisionData(-1L, List.of(), localRevisionId != -1L)
            );
        }
        
        // Revision has changed, fetch full data
        return readFullRevisionData(remoteRevisionId);
    }

    /**
     * Read complete item including tasks list when revision has changed.
     */
    private CompletionStage<ServiceRevisionData> readFullRevisionData(long revisionId) {
        QueryRequest request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("PK = :pk AND revisionId = :rev")
            .expressionAttributeValues(Map.of(
                ":pk", AttributeValue.fromS("service#" + service),
                ":rev", AttributeValue.fromN(String.valueOf(revisionId))
            ))
            .build();
            
        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                LOG.warn("Revision {} not found for service {}", revisionId, service);
                return new ServiceRevisionData(revisionId, List.of(), true);
            }
            
            Map<String, AttributeValue> item = response.items().get(0);
            List<String> tasks = extractTasksFromItem(item);
            
            boolean isNewRevision = (revisionId != localRevisionId);
            updateLocalState(revisionId, tasks);
            
            LOG.info("Read revision {} with {} tasks (new: {})", 
                revisionId, tasks.size(), isNewRevision);
            
            return new ServiceRevisionData(revisionId, tasks, isNewRevision);
        });
    }

    /**
     * Alternative method: Read latest data without revision checking.
     * Useful for initial bootstrap.
     */
    public CompletionStage<ServiceRevisionData> readLatestRevisionForce() {
        QueryRequest request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("PK = :pk")
            .expressionAttributeValues(Map.of(
                ":pk", AttributeValue.fromS("service#" + service)
            ))
            .scanIndexForward(false)  // Latest first
            .limit(1)
            .build();
            
        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                updateLocalState(-1L, List.of());
                return new ServiceRevisionData(-1L, List.of(), true);
            }
            
            Map<String, AttributeValue> item = response.items().get(0);
            long revisionId = Long.parseLong(item.get("revisionId").n());
            List<String> tasks = extractTasksFromItem(item);
            
            boolean isNewRevision = (revisionId != localRevisionId);
            updateLocalState(revisionId, tasks);
            
            return new ServiceRevisionData(revisionId, tasks, isNewRevision);
        }).exceptionally(this::handleException);
    }

    /**
     * Get specific revision by ID (useful for rollback scenarios).
     */
    public CompletionStage<Optional<ServiceRevisionData>> readSpecificRevision(long targetRevisionId) {
        QueryRequest request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("PK = :pk AND revisionId = :rev")
            .expressionAttributeValues(Map.of(
                ":pk", AttributeValue.fromS("service#" + service),
                ":rev", AttributeValue.fromN(String.valueOf(targetRevisionId))
            ))
            .build();
            
        return ddb.query(request).thenApply(response -> {
            if (response.items().isEmpty()) {
                return Optional.<ServiceRevisionData>empty();
            }
            
            Map<String, AttributeValue> item = response.items().get(0);
            List<String> tasks = extractTasksFromItem(item);
            
            return Optional.of(new ServiceRevisionData(targetRevisionId, tasks, false));
        }).exceptionally(ex -> {
            LOG.error("Error reading specific revision {}", targetRevisionId, ex);
            return Optional.<ServiceRevisionData>empty();
        });
    }

    /**
     * Extract tasks list from DynamoDB item.
     */
    private List<String> extractTasksFromItem(Map<String, AttributeValue> item) {
        AttributeValue tasksAttr = item.get("tasks");
        if (tasksAttr == null || tasksAttr.l() == null) {
            return List.of();
        }
        
        return tasksAttr.l().stream()
            .map(AttributeValue::s)
            .filter(s -> s != null && !s.isBlank())
            .toList();
    }

    /**
     * Update local state thread-safely.
     */
    private void updateLocalState(long revisionId, List<String> tasks) {
        this.localRevisionId = revisionId;
        this.cachedTasks = List.copyOf(tasks);  // Defensive copy
        this.lastReadTimestamp = clock.millis();
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
        return new ServiceRevisionData(localRevisionId, cachedTasks, false);
    }

    /**
     * Get current local state without querying DynamoDB.
     */
    public ServiceRevisionData getCachedData() {
        return new ServiceRevisionData(localRevisionId, cachedTasks, false);
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
        ddb.close();
    }
}