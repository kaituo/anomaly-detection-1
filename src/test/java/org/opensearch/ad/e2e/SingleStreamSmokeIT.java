/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.model.TaskState;

import com.google.gson.JsonObject;

/**
 * Test that is meant to run with job scheduler to test if we have at least consecutive results generated.
 *
 */
public class SingleStreamSmokeIT extends AbstractADSyntheticDataTest {

    public void testGenerateResult() throws Exception {
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetFileName());
        int intervalsToWait = 3;

        List<JsonObject> data = getData(dataFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        int trainTestSplit = 1500;
        // train data plus a few data points for real time inference
        bulkIndexTrainData(datasetName(), data, trainTestSplit + intervalsToWait + 3, ingestClient(), mapping);

        long windowDelayMinutes = getWindowDelayMinutes(data, trainTestSplit - 1, "timestamp");
        int intervalMinutes = 1;

        // single-stream detector can use window delay 0 here because we give the run api the actual data time
        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + " %s"
                    + "\"schema_version\": 0 }",
                datasetName(),
                intervalMinutes,
                windowDelayMinutes,
                customResultIndexField()
            );
        String detectorId = createDetector(client(), detector);

        startDetector(detectorId, client());

        long waitMinutes = intervalMinutes * (intervalsToWait + 1);
        // wait for scheduler to trigger AD
        Thread.sleep(Duration.ofMinutes(waitMinutes));

        List<JsonObject> results = getAnomalyResultByExecutionTime(
            detectorId,
            Instant.now(),
            1,
            client(),
            true,
            waitMinutes * 60000,
            intervalsToWait
        );

        assertTrue(
            String.format(Locale.ROOT, "Expect at least %d but got %d", intervalsToWait, results.size()),
            results.size() >= intervalsToWait
        );
    }

    /**
     * Test that is meant to check if quick start/stop would set task state to STOPPED. We have a delayed thread in the startDetector method
     * to update task state to RUNNING. Before updating task state to running, TaskManager.updateLatestRealtimeTask should set task state to
     * STOPPED if job is disabled.
     *
     */
    public void testStartStopDetector() throws Exception {
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetFileName());

        List<JsonObject> data = getData(dataFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        int trainTestSplit = 1500;
        // train data plus a few data points for real time inference
        bulkIndexTrainData(datasetName(), data, trainTestSplit + 5, ingestClient(), mapping);

        long windowDelayMinutes = getWindowDelayMinutes(data, trainTestSplit - 1, "timestamp");
        int intervalMinutes = 1;

        // single-stream detector can use window delay 0 here because we give the run api the actual data time
        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test-stop\", \"description\": \"test-stop\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + " %s"
                    + "\"schema_version\": 0 }",
                datasetName(),
                intervalMinutes,
                windowDelayMinutes,
                customResultIndexField()
            );
        String detectorId = createDetector(client(), detector);

        // Step 1: Start the detector
        startDetector(detectorId, client());

        // Step 2: Stop the detector
        stopDetector(detectorId, client());

        // Step 3: Wait for 1 minute
        Thread.sleep(Duration.ofMinutes(1).toMillis());

        // Step 4: Check if the task status is stopped
        List<JsonObject> tasks = getTasks(detectorId, 1, (hits, expectedSize) -> hits.size() >= expectedSize, client());
        assertFalse("Expected at least one task", tasks.isEmpty());

        JsonObject task = tasks.get(0);

        String taskState = task.get("state").getAsString();

        assertEquals("Task state should be STOPPED", TaskState.STOPPED.name(), taskState);
    }

    protected RestClient ingestClient() throws IOException {
        return client();
    }

    protected String datasetName() {
        return "synthetic";
    }

    protected String datasetFileName() {
        return "synthetic";
    }

    protected String customResultIndexField() {
        String tenantId = tenantId();
        if (tenantId == null || tenantId.isBlank()) {
            return "";
        }
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return String.format(Locale.ROOT, "\"result_index\": \"%s\",", resultIndex);
    }

    protected void stopDetector(String detectorId, RestClient client) throws Exception {
        Request request = tenantAwareRequest(
            "POST",
            String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_stop", detectorId)
        );
        client.performRequest(request);
    }

}
