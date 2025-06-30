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

package org.opensearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Locale;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Job;

public class AnomalyDetectorJobTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseAnomalyDetectorJob() throws IOException {
        Job anomalyDetectorJob = TestHelpers.randomJob();
        String anomalyDetectorJobString = TestHelpers
            .xContentBuilderToString(anomalyDetectorJob.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        anomalyDetectorJobString = anomalyDetectorJobString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));

        Job parsedAnomalyDetectorJob = Job.parse(TestHelpers.parser(anomalyDetectorJobString));
        assertEquals("Parsing anomaly detect result doesn't work", anomalyDetectorJob, parsedAnomalyDetectorJob);
    }

    public void testSerialization() throws IOException {
        Job anomalyDetectorJob = TestHelpers.randomJob();
        BytesStreamOutput output = new BytesStreamOutput();
        anomalyDetectorJob.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Job parsedAnomalyDetectorJob = new Job(input);
        assertNotNull(parsedAnomalyDetectorJob);
    }

    public void testParseJobWithNullWindowDelay() throws IOException {
        Job maintenanceJob = new Job(
            "HourlyCron",
            new IntervalSchedule(Instant.parse("2026-04-13T15:00:00Z"), 60, ChronoUnit.MINUTES),
            null,
            true,
            Instant.parse("2026-04-13T15:00:00Z"),
            null,
            Instant.parse("2026-04-13T15:00:00Z"),
            3600L,
            null,
            null,
            null,
            AnalysisType.HOURLY_MAINTENANCE
        );

        String jobString = TestHelpers.xContentBuilderToString(maintenanceJob.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        assertFalse(jobString.contains("\"window_delay\""));

        String legacyJobString = jobString.replaceFirst("\"enabled\":", "\"window_delay\":null,\"enabled\":");
        Job parsedJob = Job.parse(TestHelpers.parser(legacyJobString));

        assertEquals(maintenanceJob.getName(), parsedJob.getName());
        assertEquals(maintenanceJob.getSchedule(), parsedJob.getSchedule());
        assertNull(parsedJob.getWindowDelay());
        assertEquals(maintenanceJob.getAnalysisType(), parsedJob.getAnalysisType());
        assertTrue(parsedJob.isEnabled());
    }
}
