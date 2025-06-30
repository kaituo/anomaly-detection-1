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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.servicediscovery.ServiceDiscoveryAsyncClient;
import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesRequest;
import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesResponse;
import software.amazon.awssdk.services.servicediscovery.model.HealthStatusFilter;
import software.amazon.awssdk.services.servicediscovery.model.NamespaceNotFoundException;
import software.amazon.awssdk.services.servicediscovery.model.ServiceDiscoveryException;
import software.amazon.awssdk.services.servicediscovery.model.ServiceNotFoundException;

public final class CloudMapWatcher implements AutoCloseable, Runnable {

    private static final Logger LOG = LogManager.getLogger(CloudMapWatcher.class);
    private static final long COOL_DOWN_PERIOD_MS = TimeUnit.MINUTES.toMillis(10);
    // keep each historic revision for 7 days after it is written
    private static final String PK_COLUMN = "PK";
    private static final String REVISION_ID_COLUMN = "revisionId";
    private static final String CLOUD_MAP_REVISION_COLUMN = "cloudMapRevision";
    private static final String TASKS_COLUMN = "tasks";
    private static final String TTL_ATTR = "expiresAt";
    private static final String SERVICE_PK_PREFIX = "service#";
    private static final long RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(7);

    private final Region region;
    private final String namespace;
    private final String service;
    private final String tableName;
    private final boolean filterHealthyInstances;

    private final ServiceDiscoveryAsyncClient sd;
    private final DynamoDbAsyncClient ddb;

    private volatile long lastCloudMapRevision = -1L;
    private volatile long lastPublishedRevisionId = -1L;
    private volatile List<String> lastPublishedIps = List.of();
    private volatile boolean hasPublishedSnapshot = false;
    private volatile long coolDownUntilTimestamp = 0;

    private final Clock clock;

    /* ------ ctor ------ */
    public CloudMapWatcher(Settings settings, Clock clock) {
        this.region = Region.of(TimeSeriesSettings.REGION.get(settings));
        this.namespace = TimeSeriesSettings.CLOUD_MAP_NAMESPACE.get(settings);
        this.service = TimeSeriesSettings.CLOUD_MAP_SERVICE.get(settings);
        this.tableName = TimeSeriesSettings.CLOUD_MAP_TABLE_NAME.get(settings);
        this.filterHealthyInstances = TimeSeriesSettings.CLOUD_MAP_FILTER_HEALTHY_INSTANCES.get(settings);

        this.sd = doPrivileged(
            () -> ServiceDiscoveryAsyncClient.builder().region(region).credentialsProvider(SecurityUtil.createCredentialsProvider()).build()
        );

        this.ddb = doPrivileged(
            () -> DynamoDbAsyncClient.builder().region(region).credentialsProvider(SecurityUtil.createCredentialsProvider()).build()
        );

        this.clock = clock;
    }

    /* ------ poll cycle ------ */
    @Override
    public void run() {
        if (clock.millis() < coolDownUntilTimestamp) {
            LOG.debug("Cloud Map watcher is in cooldown period due to configuration errors.");
            return;
        }

        DiscoverInstancesRequest req = buildDiscoverInstancesRequest(namespace, service, filterHealthyInstances);

        sd.discoverInstances(req).thenCompose(this::handleDiscoverResponse).exceptionally(this::handleTopLevelException);
    }

    static DiscoverInstancesRequest buildDiscoverInstancesRequest(String namespace, String service, boolean filterHealthyInstances) {
        DiscoverInstancesRequest.Builder builder = DiscoverInstancesRequest.builder().namespaceName(namespace).serviceName(service);
        if (filterHealthyInstances) {
            builder.healthStatus(HealthStatusFilter.HEALTHY);
        }
        return builder.build();
    }

    /* ------ response handling ------ */
    private CompletionStage<Void> handleDiscoverResponse(DiscoverInstancesResponse resp) {
        if (coolDownUntilTimestamp > 0) {
            LOG.info("Cloud Map watcher has recovered from a configuration error state.");
            coolDownUntilTimestamp = 0;
        }

        long cloudMapRevision = resp.instancesRevision();
        List<String> ips = extractIpv4Addresses(resp);
        if (shouldSkipPublish(cloudMapRevision, ips)) {
            return CompletableFuture.completedFuture(null);
        }

        return readLatestPublishedSnapshot().thenCompose(remoteSnapshot -> {
            if (remoteSnapshot.matches(cloudMapRevision, ips)) {
                updatePublishedState(remoteSnapshot.revisionId, cloudMapRevision, ips);
                return CompletableFuture.completedFuture(null);
            }

            long revisionId = nextRevisionId(lastPublishedRevisionId, remoteSnapshot.revisionId, cloudMapRevision);
            return putRevisionItemAsync(revisionId, cloudMapRevision, ips).thenRun(() -> {
                updatePublishedState(revisionId, cloudMapRevision, ips);
                LOG
                    .info(
                        String
                            .format(
                                Locale.ROOT,
                                "Updated task list to revision %d (Cloud Map revision %d, %d workers)",
                                revisionId,
                                cloudMapRevision,
                                ips.size()
                            )
                    );
            }).exceptionally(ex -> {          // conditional-write race is not fatal
                Throwable cause = unwrap(ex);
                if (cause instanceof ConditionalCheckFailedException) {
                    // Another cluster-manager writer published the same computed revision first.
                    LOG.debug("Conditional write failed for revision {}. Another writer won.", revisionId);
                    return null;
                }
                throw new CompletionException(cause);          // escalate
            });
        });
    }

    static List<String> extractIpv4Addresses(DiscoverInstancesResponse resp) {
        return resp
            .instances()
            .stream()
            .map(i -> i.attributes().get("AWS_INSTANCE_IPV4"))
            .filter(ip -> ip != null && !ip.isBlank())
            .distinct()
            .sorted()
            .toList();
    }

    static long nextRevisionId(long localRevisionId, long remoteRevisionId, long cloudMapRevision) {
        long baseRevision = Math.max(Math.max(localRevisionId, remoteRevisionId), cloudMapRevision);
        if (baseRevision == Long.MAX_VALUE) {
            throw new IllegalStateException("Cannot allocate a new hash-ring revision after Long.MAX_VALUE");
        }
        return baseRevision + 1;
    }

    private boolean shouldSkipPublish(long cloudMapRevision, List<String> ips) {
        return hasPublishedSnapshot && cloudMapRevision == lastCloudMapRevision && ips.equals(lastPublishedIps);
    }

    private void updatePublishedState(long revisionId, long cloudMapRevision, List<String> ips) {
        lastPublishedRevisionId = revisionId;
        lastCloudMapRevision = cloudMapRevision;
        lastPublishedIps = List.copyOf(ips);
        hasPublishedSnapshot = true;
    }

    private CompletionStage<PublishedSnapshot> readLatestPublishedSnapshot() {
        QueryRequest req = QueryRequest
            .builder()
            .tableName(tableName)
            .keyConditionExpression(PK_COLUMN + " = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(SERVICE_PK_PREFIX + service)))
            .scanIndexForward(false)
            .limit(1)
            .build();

        return ddb.query(req).thenApply(response -> {
            if (response.items().isEmpty()) {
                return PublishedSnapshot.EMPTY;
            }
            return PublishedSnapshot.fromItem(response.items().get(0));
        }).exceptionally(ex -> { throw new CompletionException(unwrap(ex)); });
    }

    private static final class PublishedSnapshot {
        private static final PublishedSnapshot EMPTY = new PublishedSnapshot(-1L, -1L, List.of());

        private final long revisionId;
        private final long cloudMapRevision;
        private final List<String> ips;

        private PublishedSnapshot(long revisionId, long cloudMapRevision, List<String> ips) {
            this.revisionId = revisionId;
            this.cloudMapRevision = cloudMapRevision;
            this.ips = ips;
        }

        private static PublishedSnapshot fromItem(Map<String, AttributeValue> item) {
            return new PublishedSnapshot(
                readLong(item.get(REVISION_ID_COLUMN)),
                readLong(item.get(CLOUD_MAP_REVISION_COLUMN)),
                readStringList(item.get(TASKS_COLUMN))
            );
        }

        private boolean matches(long cloudMapRevision, List<String> ips) {
            return this.cloudMapRevision == cloudMapRevision && this.ips.equals(ips);
        }
    }

    private static long readLong(AttributeValue value) {
        return value == null || value.n() == null ? -1L : Long.parseLong(value.n());
    }

    private static List<String> readStringList(AttributeValue value) {
        if (value == null || value.l() == null) {
            return List.of();
        }
        return value.l().stream().map(AttributeValue::s).filter(s -> s != null && !s.isBlank()).distinct().sorted().toList();
    }

    /**
     * ─────────────────────────────────────────────────────────────────────────────
     * DynamoDB table: <ServiceRevisions>  (provisioned by CDK / CloudFormation)
     * ─────────────────────────────────────────────────────────────────────────────
     *
     *  ❖ Provisioning & lifecycle
     *  ---------------------------------------------------------------------------
     *  • The table is created outside this micro-service by the CDK stack
     *      (aws_dynamodb.Table construct, see infra repo).
     *  • CloudFormation guarantees the table is ACTIVE before deployment
     *      finishes, so application code can write to it immediately.
     *  • Because of this contract we **do not** call DescribeTable/CreateTable
     *      at runtime—skipping an extra control-plane round-trip and removing
     *      the need for `dynamodb:CreateTable` permission from the task role.
     *
     *  ❖ Key schema
     *  ---------------------------------------------------------------------------
     *      Partition key (HASH) : PK          – String
     *          Format: "service#${serviceName}"
     *      Example: service#echo-dns
     *
     *      Sort key (RANGE)     : revisionId  – Number
     *          Monotonically increasing AD-owned hash-ring revision.
     *          Cloud Map's instancesRevision() changes on register/deregister but
     *          not on health status changes, so we store it separately and advance
     *          revisionId whenever either that revision or the effective healthy
     *          task list changes.
     *      Example: 110844289232729806
     *
     *  ❖ Other attributes (schemaless)
     *  ---------------------------------------------------------------------------
     *      tasks : List<String>
     *          Collection of IPv4 addresses for the worker tasks discovered in
     *          Cloud Map. The cluster-manager filters to HealthStatusFilter.HEALTHY
     *          only when CLOUD_MAP_FILTER_HEALTHY_INSTANCES is enabled. No secondary indexes refer to this attribute.
     *      Example:
     *          [
     *            { "S": "172.31.49.96" },
     *            { "S": "172.31.45.13" },
     *            { "S": "172.31.13.236" }
     *          ]
     *      "S" = String for DynamoDB type.
     *
     *      cloudMapRevision : Number
     *          Raw Cloud Map instancesRevision() from the DiscoverInstances response.
     *          This is useful for diagnostics but is not used as the DynamoDB sort key
     *          because Cloud Map health-only changes do not advance it.
     *
     *      expiresAt : Number   (epoch-seconds)  ⟵ TTL attribute
     *          • Added on every write:   now() + RETENTION_SECONDS
     *          • The CDK stack enables table-wide TTL with
     *              timeToLiveAttribute = "expiresAt".
     *          • DynamoDB’s background TTL process deletes each revision
     *            automatically ≈48 h after the timestamp is reached, keeping
     *            the table size bounded without a custom cleanup job.
     *      Example: 1753824333
     *
     *  ❖ CDK snippet
     *  ---------------------------------------------------------------------------
     *      Table table = Table.Builder.create(this, "ServiceRevisions")
     *          .partitionKey(Attribute.builder()
     *              .name("PK")
     *              .type(AttributeType.STRING)
     *              .build())
     *          .sortKey(Attribute.builder()
     *              .name("revisionId")
     *              .type(AttributeType.NUMBER)
     *              .build())
     *          .billingMode(BillingMode.PAY_PER_REQUEST)
     *          // When the retention window elapses, DynamoDB deletes the item in the
     *          // background—no extra code or Lambda sweeper required.
     *          .timeToLiveAttribute(TTL_ATTR)
     *          // keep data on stack delete
     *          .removalPolicy(RemovalPolicy.RETAIN)
     *          .build();
     *
     *  Any write in this class therefore assumes the table exists and is ACTIVE.
     *  If the table is accidentally deleted, the service will fail fast with
     *  ResourceNotFoundException, signaling ops to redeploy the stack instead of
     *  silently recreating an un-tagged, un-encrypted table at runtime.
     * ─────────────────────────────────────────────────────────────────────────────
     */
    private CompletionStage<Void> putRevisionItemAsync(long revisionId, long cloudMapRevision, List<String> ips) {
        long nowSeconds = clock.instant().getEpochSecond();
        long ttlSeconds = nowSeconds + RETENTION_SECONDS;

        PutItemRequest req = PutItemRequest
            .builder()
            .tableName(tableName)
            .item(
                Map
                    .of(
                        PK_COLUMN,
                        AttributeValue.fromS(SERVICE_PK_PREFIX + service),
                        REVISION_ID_COLUMN,
                        AttributeValue.fromN(Long.toString(revisionId)),
                        CLOUD_MAP_REVISION_COLUMN,
                        AttributeValue.fromN(Long.toString(cloudMapRevision)),
                        TASKS_COLUMN,
                        AttributeValue.fromL(ips.stream().map(AttributeValue::fromS).toList()),
                        TTL_ATTR,
                        AttributeValue.fromN(Long.toString(ttlSeconds))
                    )
            )
            .conditionExpression("attribute_not_exists(" + REVISION_ID_COLUMN + ") or " + REVISION_ID_COLUMN + " < :rev")
            .expressionAttributeValues(Map.of(":rev", AttributeValue.fromN(Long.toString(revisionId))))
            .build();
        return ddb.putItem(req).thenAccept(r -> {});
    }

    /* ------ top-level error handling ------ */
    private Void handleTopLevelException(Throwable ex) {
        Throwable cause = unwrap(ex);
        if (cause instanceof ServiceNotFoundException
            || cause instanceof NamespaceNotFoundException
            || cause instanceof ResourceNotFoundException) {
            LOG.error("Configuration error in Cloud Map watcher. Entering 10-minute cooldown.", cause);
            coolDownUntilTimestamp = clock.millis() + COOL_DOWN_PERIOD_MS;
        } else if (cause instanceof ServiceDiscoveryException
            || cause instanceof DynamoDbException
            || cause instanceof SdkClientException) {
            LOG.warn("Transient error in Cloud Map watcher. Will retry later.", cause);
        } else {
            LOG.error("Unexpected error in Cloud Map poll cycle.", cause);
        }
        return null;
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null) ? ce.getCause() : t;
    }

    /* ------ graceful shutdown (fire-and-forget) ------ */
    @Override
    public void close() {
        sd.close();   // both return CompletableFuture<Void>
        ddb.close();  // let the event-loop shut down asynchronously
    }
}
