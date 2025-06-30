/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cloudwatch;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class HeapUsageMetricPublisher implements Closeable {
    public static final String HEAP_USED_PERCENT_METRIC = "HeapMemoryUsedPercent";
    public static final String HEAP_USED_BYTES_METRIC = "HeapMemoryUsedBytes";
    public static final String CLUSTER_NAME_DIMENSION = "ClusterName";
    public static final String SERVICE_NAME_DIMENSION = "ServiceName";
    public static final String ROLE_DIMENSION = "Role";

    private static final Logger LOG = LogManager.getLogger(HeapUsageMetricPublisher.class);

    private final String namespace;
    private final String role;
    private final JvmService jvmService;
    private final CloudWatchClient cloudWatchClient;
    private final List<Dimension> dimensions;
    private final Cancellable cancellable;

    public static HeapUsageMetricPublisher createIfEnabled(Settings settings, ThreadPool threadPool, JvmService jvmService) {
        if (TimeSeriesSettings.CLOUDWATCH_HEAP_METRICS_ENABLED.get(settings) == false) {
            return null;
        }

        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (roles.contains(TimeSeriesSettings.MODEL_ROLE) == false) {
            return null;
        }

        String clusterName = TimeSeriesSettings.CLOUDWATCH_METRICS_CLUSTER_NAME.get(settings);
        String serviceName = TimeSeriesSettings.cloudWatchMetricsServiceName(settings, TimeSeriesSettings.MODEL_ROLE);
        String region = TimeSeriesSettings.REGION.get(settings);
        if (clusterName == null
            || clusterName.isBlank()
            || serviceName == null
            || serviceName.isBlank()
            || region == null
            || region.isBlank()) {
            LOG
                .warn(
                    "CloudWatch heap metric publishing is enabled but cluster_name, service_name for model, "
                        + "or region is empty; publisher will not start."
                );
            return null;
        }

        return new HeapUsageMetricPublisher(
            TimeSeriesSettings.CLOUDWATCH_METRICS_NAMESPACE.get(settings),
            clusterName,
            serviceName,
            roles.contains(TimeSeriesSettings.MODEL_ROLE) ? TimeSeriesSettings.MODEL_ROLE : TimeSeriesSettings.COORDINATOR_ROLE,
            Region.of(region),
            TimeSeriesSettings.CLOUDWATCH_HEAP_METRICS_INTERVAL.get(settings),
            threadPool,
            jvmService
        );
    }

    private HeapUsageMetricPublisher(
        String namespace,
        String clusterName,
        String serviceName,
        String role,
        Region region,
        TimeValue interval,
        ThreadPool threadPool,
        JvmService jvmService
    ) {
        this.namespace = namespace;
        this.role = role;
        this.jvmService = jvmService;
        this.cloudWatchClient = doPrivileged(
            () -> CloudWatchClient
                .builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(region)
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .build()
        );
        this.dimensions = List
            .of(
                Dimension.builder().name(CLUSTER_NAME_DIMENSION).value(clusterName).build(),
                Dimension.builder().name(SERVICE_NAME_DIMENSION).value(serviceName).build(),
                Dimension.builder().name(ROLE_DIMENSION).value(role).build()
            );
        this.cancellable = threadPool.scheduleWithFixedDelay(this::publishSafely, interval, ThreadPool.Names.GENERIC);
        publishSafely();
    }

    private void publishSafely() {
        try {
            JvmStats.Mem mem = jvmService.stats().getMem();
            short heapUsedPercent = mem.getHeapUsedPercent();
            if (heapUsedPercent < 0) {
                LOG.debug("Skipping CloudWatch heap metric publish because heap max is unknown for role [{}].", role);
                return;
            }
            cloudWatchClient
                .putMetricData(
                    PutMetricDataRequest
                        .builder()
                        .namespace(namespace)
                        .metricData(
                            MetricDatum
                                .builder()
                                .metricName(HEAP_USED_PERCENT_METRIC)
                                .unit(StandardUnit.PERCENT)
                                .value((double) heapUsedPercent)
                                .dimensions(dimensions)
                                .build(),
                            MetricDatum
                                .builder()
                                .metricName(HEAP_USED_BYTES_METRIC)
                                .unit(StandardUnit.BYTES)
                                .value((double) mem.getHeapUsed().getBytes())
                                .dimensions(dimensions)
                                .build()
                        )
                        .build()
                );
        } catch (Exception e) {
            LOG.debug("Failed to publish CloudWatch heap metrics for role [{}].", role, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
        cloudWatchClient.close();
    }
}
