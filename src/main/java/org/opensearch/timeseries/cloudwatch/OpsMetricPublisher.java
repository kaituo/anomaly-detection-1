/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * Publishes cell-level detector-execution counters to CloudWatch so operational alarms can compute an
 * execution error <b>rate</b> ({@code DetectorExecutionFailures / DetectorExecutions}) for the cell.
 *
 * <p>Sibling of {@link HeapUsageMetricPublisher} / {@link SqsMetricPublisher}: gated by a
 * {@code plugins.timeseries.cloudwatch.ops_metrics.*} setting, runs only on the coordinator role,
 * builds its CloudWatch client the same way (privileged, URL-connection HTTP client,
 * {@link SecurityUtil} credentials), and is registered as a {@link Closeable} component so the node
 * lifecycle stops it.
 *
 * <p>The detector index records detector <i>configs</i>, not how many runs happened or failed. The
 * plugin already maintains those as node stats ({@link StatNames#AD_EXECUTE_REQUEST_COUNT} /
 * {@link StatNames#AD_EXECUTE_FAIL_COUNT} and their HC variants). These are monotonic cumulative
 * counters, so each flush emits {@code current - lastEmitted} (a counter that goes backwards after a
 * restart is treated as {@code current}). Executions and failures are emitted independently — a
 * metric with a zero delta for the interval is skipped rather than emitting a zero datum.
 *
 * <p>Emitted under the namespace from {@link TimeSeriesSettings#CLOUDWATCH_OPS_METRICS_NAMESPACE}
 * (the deployment overrides the open-source-safe default {@link #DEFAULT_NAMESPACE} via {@code -E}),
 * with dimensions {@code {Stage, Region, CellId}}:
 * <ul>
 *   <li>{@code DetectorExecutions}        = Δ(ad_execute_request_count) + Δ(ad_hc_execute_request_count)</li>
 *   <li>{@code DetectorExecutionFailures} = Δ(ad_execute_failure_count) + Δ(ad_hc_execute_failure_count)</li>
 * </ul>
 */
public class OpsMetricPublisher implements Closeable {

    /** Open-source-safe default; the OASIS deployment overrides it via {@code -E} (see {@code oasis}). */
    public static final String DEFAULT_NAMESPACE = "OpenSearch/AnomalyDetection";

    public static final String METRIC_EXECUTIONS = "DetectorExecutions";
    public static final String METRIC_EXECUTION_FAILURES = "DetectorExecutionFailures";

    public static final String DIMENSION_STAGE = "Stage";
    public static final String DIMENSION_REGION = "Region";
    public static final String DIMENSION_CELL_ID = "CellId";

    private static final Logger LOG = LogManager.getLogger(OpsMetricPublisher.class);
    private static final String UNKNOWN = "unknown";

    /** Node-level execution counters read each flush; deltas summed into the two emitted metrics. */
    private static final String[] REQUEST_STATS = {
        StatNames.AD_EXECUTE_REQUEST_COUNT.getName(),
        StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName() };
    private static final String[] FAILURE_STATS = {
        StatNames.AD_EXECUTE_FAIL_COUNT.getName(),
        StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName() };

    private final Stats stats;
    private final String namespace;
    private final List<Dimension> dimensions;
    private final CloudWatchClient cloudWatchClient;
    /** Last cumulative value emitted per stat name, for the read-and-delta computation. */
    private final Map<String, Long> lastEmitted = new ConcurrentHashMap<>();
    private Cancellable cancellable;

    public static OpsMetricPublisher createIfEnabled(Settings settings, ThreadPool threadPool, Stats stats) {
        if (TimeSeriesSettings.CLOUDWATCH_OPS_METRICS_ENABLED.get(settings) == false) {
            return null;
        }

        // Detector execution counters are cluster-wide work; publish once from the coordinator.
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (roles.contains(TimeSeriesSettings.COORDINATOR_ROLE) == false) {
            return null;
        }

        if (stats == null) {
            LOG.warn("CloudWatch ops metric publishing is enabled but stats are unavailable; publisher will not start.");
            return null;
        }

        String region = TimeSeriesSettings.REGION.get(settings);
        if (region == null || region.isBlank()) {
            LOG.warn("CloudWatch ops metric publishing is enabled but region is empty; publisher will not start.");
            return null;
        }
        String stage = blankToUnknown(TimeSeriesSettings.DOMAIN.get(settings));
        String cellId = blankToUnknown(TimeSeriesSettings.CLOUDWATCH_METRICS_CELL_ID.get(settings));
        String namespace = TimeSeriesSettings.CLOUDWATCH_OPS_METRICS_NAMESPACE.get(settings);

        CloudWatchClient client = doPrivileged(
            () -> CloudWatchClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(region))
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        );

        return new OpsMetricPublisher(
            stats,
            namespace,
            stage,
            region,
            cellId,
            client,
            TimeSeriesSettings.CLOUDWATCH_OPS_METRICS_INTERVAL.get(settings),
            threadPool
        );
    }

    private OpsMetricPublisher(
        Stats stats,
        String namespace,
        String stage,
        String region,
        String cellId,
        CloudWatchClient client,
        TimeValue interval,
        ThreadPool threadPool
    ) {
        this(stats, namespace, stage, region, cellId, client);
        this.cancellable = threadPool.scheduleWithFixedDelay(this::publishSafely, interval, ThreadPool.Names.GENERIC);
        LOG.info("OpsMetricPublisher started; namespace={} stage={} region={} cellId={} interval={}", namespace, stage, region, cellId, interval);
        publishSafely();
    }

    /** Visible for testing: build a publisher around an injected client without scheduling. */
    OpsMetricPublisher(Stats stats, String namespace, String stage, String region, String cellId, CloudWatchClient client) {
        this.stats = stats;
        this.namespace = namespace;
        this.cloudWatchClient = client;
        this.dimensions = List
            .of(
                Dimension.builder().name(DIMENSION_STAGE).value(stage).build(),
                Dimension.builder().name(DIMENSION_REGION).value(region).build(),
                Dimension.builder().name(DIMENSION_CELL_ID).value(cellId).build()
            );
    }

    /**
     * Reads the execution counters, computes per-interval deltas, and emits {@code DetectorExecutions}
     * / {@code DetectorExecutionFailures} independently for the cell. Never throws.
     */
    void publishSafely() {
        try {
            long executions = sumDeltas(REQUEST_STATS);
            long failures = sumDeltas(FAILURE_STATS);

            List<MetricDatum> data = new ArrayList<>(2);
            if (executions > 0L) {
                data.add(datum(METRIC_EXECUTIONS, executions));
            }
            if (failures > 0L) {
                data.add(datum(METRIC_EXECUTION_FAILURES, failures));
            }
            if (data.isEmpty()) {
                return;
            }

            for (MetricDatum d : data) {
                LOG.info("Publishing ops metric namespace={} {}{} = {}", namespace, d.metricName(), dimensionString(), d.value());
            }
            cloudWatchClient.putMetricData(PutMetricDataRequest.builder().namespace(namespace).metricData(data).build());
        } catch (Exception e) {
            LOG.warn("Failed to publish AD detector execution metrics to CloudWatch", e);
        }
    }

    /** Sums the per-interval delta of each named cumulative counter, updating the last-emitted map. */
    private long sumDeltas(String[] statNames) {
        long total = 0L;
        for (String name : statNames) {
            Long current = readStat(name);
            if (current == null) {
                continue;
            }
            long last = lastEmitted.getOrDefault(name, 0L);
            // Counter went backwards (node restart reset the in-memory counter): treat last as 0.
            long delta = current >= last ? current - last : current;
            lastEmitted.put(name, current);
            total += delta;
        }
        return total;
    }

    /** Reads a single cumulative counter value; null if the stat is absent or non-numeric. */
    private Long readStat(String name) {
        try {
            Object value = stats.getStat(name).getValue();
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            LOG.warn("AD stat {} is not numeric ({}); skipping", name, value);
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to read AD stat {}; skipping this flush", name, e);
            return null;
        }
    }

    private MetricDatum datum(String name, long value) {
        return MetricDatum
            .builder()
            .metricName(name)
            .dimensions(dimensions)
            .value((double) value)
            .unit(StandardUnit.COUNT)
            .build();
    }

    private String dimensionString() {
        StringBuilder sb = new StringBuilder("{");
        for (Dimension d : dimensions) {
            if (sb.length() > 1) {
                sb.append(',');
            }
            sb.append(d.name()).append('=').append(d.value());
        }
        return sb.append('}').toString();
    }

    private static String blankToUnknown(String value) {
        return (value == null || value.isBlank()) ? UNKNOWN : value;
    }

    /** Current last-emitted value tracked for a stat, 0 if none. Visible for testing. */
    long peekLastForTest(String statName) {
        return lastEmitted.getOrDefault(statName, 0L);
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
        }
    }
}
