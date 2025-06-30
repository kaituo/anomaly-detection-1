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
    private static final String TTL_ATTR = "expiresAt";
    private static final long RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(7);

    private final Region region;
    private final String namespace;
    private final String service;
    private final String tableName;

    private final ServiceDiscoveryAsyncClient sd;
    private final DynamoDbAsyncClient ddb;

    private volatile long lastRevisionId = -1L;
    private volatile long coolDownUntilTimestamp = 0;

    private final Clock clock;

    /* ------ ctor ------ */
    public CloudMapWatcher(Settings settings, Clock clock) {
        this.region = Region.of(TimeSeriesSettings.REGION.get(settings));
        this.namespace = TimeSeriesSettings.CLOUD_MAP_NAMESPACE.get(settings);
        this.service = TimeSeriesSettings.CLOUD_MAP_SERVICE.get(settings);
        this.tableName = TimeSeriesSettings.CLOUD_MAP_TABLE_NAME.get(settings);

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

        DiscoverInstancesRequest req = DiscoverInstancesRequest
            .builder()
            .namespaceName(namespace)
            .serviceName(service)
            .healthStatus(HealthStatusFilter.HEALTHY)
            .build();

        sd.discoverInstances(req).thenCompose(this::handleDiscoverResponse).exceptionally(this::handleTopLevelException);
    }

    /* ------ response handling ------ */
    private CompletionStage<Void> handleDiscoverResponse(DiscoverInstancesResponse resp) {
        if (coolDownUntilTimestamp > 0) {
            LOG.info("Cloud Map watcher has recovered from a configuration error state.");
            coolDownUntilTimestamp = 0;
        }

        long revision = resp.instancesRevision();
        if (revision == lastRevisionId) {
            return CompletableFuture.completedFuture(null);   // nothing to do
        }

        List<String> ips = resp
            .instances()
            .stream()
            .map(i -> i.attributes().get("AWS_INSTANCE_IPV4"))
            .filter(ip -> ip != null && !ip.isBlank())
            .toList();

        return putRevisionItemAsync(revision, ips).thenRun(() -> {
            lastRevisionId = revision;
            LOG.info(String.format(Locale.ROOT, "Updated task list to revision %d (%d workers)", revision, ips.size()));
        }).exceptionally(ex -> {          // conditional-write race is not fatal
            Throwable cause = unwrap(ex);
            if (cause instanceof ConditionalCheckFailedException) {
                // at the beginning when the node restarts, the lastRevisionId on file will be -1, this will
                // trigger one write to the table. But if cloudmap does not change, the recorded revisionId 
                // equals to the revision to be written, this will trigger ConditionalCheckFailedException.
                LOG.debug("Conditional write failed for revision {}. Another writer won.", revision);
                lastRevisionId = revision;
                return null;
            }
            throw new RuntimeException(cause);          // escalate
        });
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
     *          Monotonically increasing Cloud Map instancesRevision().
     *          Storing more than one revision lets us examine or restore a previous
     *          task list if the newest deployment is unhealthy. Also, fetching
     *          “the latest” is efficient: Query the partition key with
     *          ScanIndexForward=false & Limit=1 to get the highest sort-key item.
     *      Example: 110844289232729805
     *
     *  ❖ Other attributes (schemaless)
     *  ---------------------------------------------------------------------------
     *      tasks : List<String>
     *          Collection of IPv4 addresses for the worker tasks discovered in
     *          Cloud Map.  No secondary indexes refer to this attribute.
     *      Example:
     *          [
     *            { "S": "172.31.49.96" },
     *            { "S": "172.31.45.13" },
     *            { "S": "172.31.13.236" }
     *          ]
     *      "S" = String for DynamoDB type.
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
    private CompletionStage<Void> putRevisionItemAsync(long revision, List<String> ips) {
        long nowSeconds = clock.instant().getEpochSecond();
        long ttlSeconds = nowSeconds + RETENTION_SECONDS;

        PutItemRequest req = PutItemRequest
            .builder()
            .tableName(tableName)
            .item(
                Map
                    .of(
                        "PK",
                        AttributeValue.fromS("service#" + service),
                        "revisionId",
                        AttributeValue.fromN(Long.toString(revision)),
                        "tasks",
                        AttributeValue.fromL(ips.stream().map(AttributeValue::fromS).toList()),
                        TTL_ATTR,
                        AttributeValue.fromN(Long.toString(ttlSeconds))
                    )
            )
            .conditionExpression("attribute_not_exists(revisionId) or revisionId < :rev")
            .expressionAttributeValues(Map.of(":rev", AttributeValue.fromN(Long.toString(revision))))
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
