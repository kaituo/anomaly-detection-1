/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.ScanBy;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class SqsMetricPublisher implements Closeable {
    public static final String TOTAL_SQS_BACKLOG_DELETED_PRESSURE_METRIC = "TotalSqsBacklogDeletedPressure";

    private static final Logger LOG = LogManager.getLogger(SqsMetricPublisher.class);
    private static final String ARN_PREFIX = "arn:";
    private static final String SQS_CLOUDWATCH_NAMESPACE = "AWS/SQS";
    private static final String SQS_QUEUE_NAME_DIMENSION = "QueueName";
    private static final String SQS_MESSAGES_DELETED_METRIC = "NumberOfMessagesDeleted";
    private static final String SQS_MESSAGES_VISIBLE_METRIC = "ApproximateNumberOfMessagesVisible";
    private static final int SQS_CLOUDWATCH_PERIOD_SECONDS = 60;

    private final String namespace;
    private final List<String> queueNames;
    private final JobQueueAccountIdProvider accountProvider;
    private final String region;
    private final String scheduleManagementRoleName;
    private final CloudWatchClient publishingCloudWatchClient;
    private final Map<String, CloudWatchClient> sourceCloudWatchClients = new ConcurrentHashMap<>();
    private final List<Dimension> dimensions;
    private final Cancellable cancellable;

    public static SqsMetricPublisher createIfEnabled(Settings settings, ThreadPool threadPool) {
        return createIfEnabled(settings, threadPool, null);
    }

    public static SqsMetricPublisher createIfEnabled(Settings settings, ThreadPool threadPool, JobQueueAccountIdProvider accountProvider) {
        if (TimeSeriesSettings.CLOUDWATCH_SQS_METRICS_ENABLED.get(settings) == false) {
            return null;
        }

        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (roles.contains(TimeSeriesSettings.MASTER_ROLE) == false) {
            return null;
        }

        String clusterName = TimeSeriesSettings.CLOUDWATCH_METRICS_CLUSTER_NAME.get(settings);
        String serviceName = TimeSeriesSettings.CLOUDWATCH_METRICS_SERVICE_NAME.get(settings);
        String region = TimeSeriesSettings.REGION.get(settings);
        String scheduleManagementRoleName = settings.get(AnomalyDetectorSettings.EVENT_BRIDGE_SCHEDULE_MANAGEMENT_ROLE_NAME.getKey());
        scheduleManagementRoleName = scheduleManagementRoleName == null ? null : scheduleManagementRoleName.trim();
        if (clusterName == null
            || clusterName.isBlank()
            || serviceName == null
            || serviceName.isBlank()
            || region == null
            || region.isBlank()
            || scheduleManagementRoleName == null
            || scheduleManagementRoleName.isBlank()) {
            LOG
                .warn(
                    "CloudWatch SQS metric publishing is enabled but cluster_name, service_name, region, "
                        + "or schedule_management_role_name is empty."
                );
            return null;
        }
        if (scheduleManagementRoleName.startsWith(ARN_PREFIX)) {
            LOG.warn("CloudWatch SQS metric publishing requires schedule_management_role_name to be a role name, not an ARN.");
            return null;
        }

        JobQueueAccountIdProvider provider = accountProvider != null
            ? accountProvider
            : JobQueueAccountIdProvider.find(TimeSeriesSettings.SQS_ACCOUNT_PROVIDER_TYPE.get(settings), settings);

        return new SqsMetricPublisher(
            TimeSeriesSettings.CLOUDWATCH_METRICS_NAMESPACE.get(settings),
            clusterName,
            serviceName,
            AnomalyDetectorSettings.SQS_QUEUE_NAME.get(settings),
            AnomalyDetectorSettings.SQS_EXTRA_QUEUE_NAMES.get(settings),
            provider,
            scheduleManagementRoleName,
            Region.of(region),
            TimeSeriesSettings.CLOUDWATCH_SQS_METRICS_INTERVAL.get(settings),
            threadPool
        );
    }

    private SqsMetricPublisher(
        String namespace,
        String clusterName,
        String serviceName,
        String queueName,
        List<String> extraQueueNames,
        JobQueueAccountIdProvider accountProvider,
        String scheduleManagementRoleName,
        Region region,
        TimeValue interval,
        ThreadPool threadPool
    ) {
        this.namespace = namespace;
        this.queueNames = queueNames(queueName, extraQueueNames);
        this.accountProvider = accountProvider;
        this.region = region.id();
        this.scheduleManagementRoleName = scheduleManagementRoleName;
        this.publishingCloudWatchClient = doPrivileged(
            () -> CloudWatchClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(region)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        );
        this.dimensions = List
            .of(
                Dimension.builder().name(HeapUsageMetricPublisher.CLUSTER_NAME_DIMENSION).value(clusterName).build(),
                Dimension.builder().name(HeapUsageMetricPublisher.SERVICE_NAME_DIMENSION).value(serviceName).build(),
                // This publisher runs on the master role but emits the coordinator scaling metric.
                Dimension.builder().name(HeapUsageMetricPublisher.ROLE_DIMENSION).value(TimeSeriesSettings.COORDINATOR_ROLE).build()
            );
        this.cancellable = threadPool.scheduleWithFixedDelay(this::publishSafely, interval, ThreadPool.Names.GENERIC);
        publishSafely();
    }

    static List<String> queueNames(String primaryQueueName, List<String> extraQueueNames) {
        List<String> names = new ArrayList<>();
        addQueueName(names, primaryQueueName);
        if (extraQueueNames != null) {
            extraQueueNames.forEach(name -> addQueueName(names, name));
        }
        return List.copyOf(names);
    }

    private static void addQueueName(List<String> names, String name) {
        if (name == null) {
            return;
        }
        String trimmed = name.trim();
        if (trimmed.isBlank() || names.contains(trimmed)) {
            return;
        }
        names.add(trimmed);
    }

    private void publishSafely() {
        List<String> accountIds = List.of();
        try {
            accountIds = currentAccountIds();
            if (accountIds.isEmpty()) {
                LOG.debug("Skipping CloudWatch SQS metric publishing because no SQS account IDs are available.");
                return;
            }
            SqsTotals totals = readLatestSqsTotals(accountIds);
            double backlogDeletedPressure = pressure(totals.backlog, totals.deleted);
            publishingCloudWatchClient
                .putMetricData(
                    PutMetricDataRequest
                        .builder()
                        .namespace(namespace)
                        .metricData(
                            MetricDatum
                                .builder()
                                .metricName(TOTAL_SQS_BACKLOG_DELETED_PRESSURE_METRIC)
                                .unit(StandardUnit.NONE)
                                .value(backlogDeletedPressure)
                                .dimensions(dimensions)
                                .build()
                        )
                        .build()
                );
        } catch (Exception e) {
            LOG.debug("Failed to publish aggregate SQS throughput metric for queues {} accounts {}.", queueNames, accountIds, e);
        }
    }

    private List<String> currentAccountIds() {
        try {
            List<String> ids = accountProvider.getAccountIds();
            return ids == null
                ? List.of()
                : ids.stream().filter(id -> id != null && id.isBlank() == false).map(String::trim).distinct().toList();
        } catch (RuntimeException e) {
            LOG.debug("Failed to load SQS account IDs from provider [{}]; skipping publish.", accountProvider.getType(), e);
            return List.of();
        }
    }

    private SqsTotals readLatestSqsTotals(List<String> accountIds) {
        if (accountIds.isEmpty()) {
            return new SqsTotals(0.0d, 0.0d);
        }

        double deleted = 0.0d;
        double backlog = 0.0d;
        for (String accountId : accountIds) {
            for (String queueName : queueNames) {
                SqsTotals accountTotals = readLatestSqsTotals(accountId, queueName);
                deleted += accountTotals.deleted;
                backlog += accountTotals.backlog;
            }
        }

        LOG
            .debug(
                "Publishing aggregate SQS pressure for queues {}, accounts {}: deleted={} backlog={}",
                queueNames,
                accountIds,
                deleted,
                backlog
            );
        return new SqsTotals(deleted, backlog);
    }

    private SqsTotals readLatestSqsTotals(String accountId, String queueName) {
        List<MetricDataQuery> queries = new ArrayList<>();
        String deletedId = "d0";
        String backlogId = "b0";
        queries.add(sqsMetricQuery(deletedId, SQS_MESSAGES_DELETED_METRIC, "Sum", queueName));
        queries.add(sqsMetricQuery(backlogId, SQS_MESSAGES_VISIBLE_METRIC, "Average", queueName));

        Instant now = Instant.now();
        var response = sourceCloudWatchClient(accountId)
            .getMetricData(
                GetMetricDataRequest
                    .builder()
                    .metricDataQueries(queries)
                    .startTime(now.minusSeconds(10L * SQS_CLOUDWATCH_PERIOD_SECONDS))
                    .endTime(now.minusSeconds(SQS_CLOUDWATCH_PERIOD_SECONDS))
                    .scanBy(ScanBy.TIMESTAMP_DESCENDING)
                    .maxDatapoints(queries.size())
                    .build()
            );

        double deleted = 0.0d;
        double backlog = 0.0d;
        for (MetricDataResult result : response.metricDataResults()) {
            if (result.values().isEmpty()) {
                continue;
            }
            if (deletedId.equals(result.id())) {
                deleted += result.values().get(0);
            } else if (backlogId.equals(result.id())) {
                backlog += result.values().get(0);
            }
        }
        return new SqsTotals(deleted, backlog);
    }

    private static double pressure(double numerator, double deleted) {
        return deleted > 0 ? numerator / deleted : numerator > 0 ? 10.0d : 0.0d;
    }

    private MetricDataQuery sqsMetricQuery(String id, String metricName, String stat, String queueName) {
        return MetricDataQuery
            .builder()
            .id(id)
            .returnData(true)
            .metricStat(
                MetricStat
                    .builder()
                    .stat(stat)
                    .period(SQS_CLOUDWATCH_PERIOD_SECONDS)
                    .unit(StandardUnit.COUNT)
                    .metric(
                        Metric
                            .builder()
                            .namespace(SQS_CLOUDWATCH_NAMESPACE)
                            .metricName(metricName)
                            .dimensions(Dimension.builder().name(SQS_QUEUE_NAME_DIMENSION).value(queueName).build())
                            .build()
                    )
                    .build()
            )
            .build();
    }

    private CloudWatchClient sourceCloudWatchClient(String accountId) {
        return sourceCloudWatchClients.computeIfAbsent(accountId, this::createSourceCloudWatchClient);
    }

    private CloudWatchClient createSourceCloudWatchClient(String accountId) {
        String roleArn = "arn:aws:iam::" + accountId + ":role/" + scheduleManagementRoleName;
        return doPrivileged(
            () -> CloudWatchClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(region))
                .credentialsProvider(SecurityUtil.createAssumeRoleCredentialsProvider(region, roleArn, "ad-sqs-cw-" + accountId))
                .build()
        );
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
        publishingCloudWatchClient.close();
        sourceCloudWatchClients.values().forEach(CloudWatchClient::close);
        sourceCloudWatchClients.clear();
    }

    private static final class SqsTotals {
        private final double deleted;
        private final double backlog;

        private SqsTotals(double deleted, double backlog) {
            this.deleted = deleted;
            this.backlog = backlog;
        }
    }
}
