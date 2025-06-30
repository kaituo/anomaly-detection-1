/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.HttpHost;
import org.junit.After;
import org.junit.BeforeClass;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

import com.google.gson.JsonObject;

public abstract class MissingIT extends AbstractADSyntheticDataTest {
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";
    protected static double min = 200.0;
    protected static double max = 240.0;
    protected static int dataSize = 400;

    protected static List<Double> randomDoubles;
    protected static String datasetName = "missing";

    protected int intervalMinutes = 10;
    public long intervalMillis = intervalMinutes * 60000L;
    protected String categoricalField = "cityName";
    protected int maxError = 20;
    protected int trainTestSplit = 100;

    public int continuousImputeStartIndex = 11;
    public int continuousImputeEndIndex = 35;

    protected Map<String, Double> lastSeen = new HashMap<>();
    private RestClient modelDataClient;

    @BeforeClass
    public static void setUpOnce() {
        // Generate the list of doubles
        randomDoubles = generateUniformRandomDoubles(dataSize, min, max);
    }

    @After
    public void closeModelDataClient() throws IOException {
        if (modelDataClient != null) {
            modelDataClient.close();
            modelDataClient = null;
        }
    }

    protected void verifyImputation(
        ImputationMethod imputation,
        Map<String, Double> lastSeen,
        double dataValue,
        JsonObject imputed0,
        String entity
    ) {
        assertTrue(imputed0.get("imputed").getAsBoolean());
        switch (imputation) {
            case ZERO:
                assertEquals(0, dataValue, EPSILON);
                break;
            case PREVIOUS:
                // if we have recorded lastSeen
                Double entityValue = lastSeen.get(entity);
                if (entityValue != null && !areDoublesEqual(entityValue, -1)) {
                    assertEquals(entityValue, dataValue, EPSILON);
                }
                break;
            case FIXED_VALUES:
                assertEquals(1, dataValue, EPSILON);
                break;
            default:
                assertTrue(false);
                break;
        }
    }

    protected TrainResult createAndStartRealTimeDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis,
        String name
    ) throws Exception {
        TrainResult trainResult = createDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis, name);
        List<JsonObject> result = startRealTimeDetector(trainResult, numberOfEntities, intervalMinutes, true);
        recordLastSeenFromResult(result);

        return trainResult;
    }

    protected TrainResult createAndStartRealTimeDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        return createAndStartRealTimeDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis, "test");
    }

    protected TrainResult createAndStartHistoricalDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        TrainResult trainResult = createDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis);
        List<JsonObject> result = startHistoricalDetector(trainResult, numberOfEntities, intervalMinutes, true);
        recordLastSeenFromResult(result);

        return trainResult;
    }

    protected void recordLastSeenFromResult(List<JsonObject> result) {
        for (int j = 0; j < result.size(); j++) {
            JsonObject source = result.get(j);
            lastSeen.put(getEntity(source), extractFeatureValue(source));
        }
    }

    protected TrainResult createDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis,
        String name
    ) throws Exception {
        Instant trainTime = Instant.ofEpochMilli(trainTimeMillis);

        Duration windowDelay = getWindowDelay(trainTimeMillis);
        String detector = genDetector(trainTestSplit, windowDelay.toMinutes(), hc, imputation, trainTimeMillis, uniqueDetectorName(name));

        RestClient client = client();
        String detectorId = createDetector(client, detector);
        LOG.info("Created detector {}", detectorId);

        return new TrainResult(detectorId, data, trainTestSplit * numberOfEntities, windowDelay, trainTime, "timestamp");
    }

    protected String uniqueDetectorName(String baseName) {
        return baseName + "-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
    }

    protected TrainResult createDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        return createDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis, "test");
    }

    protected Duration getWindowDelay(long trainTimeMillis) {
        /*
         * AD accepts windowDelay in the unit of minutes. Thus, we need to convert the delay in minutes. This will
         * make it easier to search for results based on data end time. Otherwise, real data time and the converted
         * data time from request time.
         * Assume x = real data time. y= real window delay. y'= window delay in minutes. If y and y' are different,
         * x + y - y' != x.
         */
        long currentTime = System.currentTimeMillis();
        long windowDelayMinutes = (trainTimeMillis - currentTime) / 60000;
        LOG.info("train time {}, current time {}, window delay {}", trainTimeMillis, currentTime, windowDelayMinutes);
        return Duration.ofMinutes(windowDelayMinutes);
    }

    protected RestClient ingestClient() throws IOException {
        return client();
    }

    protected String datasetName() {
        return datasetName;
    }

    protected String customResultIndexField() {
        String tenantId = tenantId();
        if (tenantId == null || tenantId.isBlank()) {
            return "";
        }
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return String.format(Locale.ROOT, "\"result_index\": \"%s\",", resultIndex);
    }

    protected final RestClient modelDataClient() throws IOException {
        if (modelDataClient == null) {
            modelDataClient = buildClient(restClientSettings(), modelHosts());
        }
        return modelDataClient;
    }

    private HttpHost[] modelHosts() {
        String cluster = System.getProperty(MODEL_CLUSTER_PROPERTY);
        if (cluster == null || cluster.isBlank()) {
            throw new IllegalStateException("Must specify [" + MODEL_CLUSTER_PROPERTY + "] to run " + getClass().getSimpleName());
        }

        String[] stringUrls = cluster.split(",");
        HttpHost[] hosts = new HttpHost[stringUrls.length];
        for (int i = 0; i < stringUrls.length; i++) {
            String stringUrl = stringUrls[i].trim();
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
            hosts[i] = buildHttpHost(host, port);
        }
        return hosts;
    }

    protected void ingestUniformSingleFeatureData(int ingestDataSize, List<JsonObject> data) throws Exception {
        String mapping = String
            .format(
                Locale.ROOT,
                "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\":"
                    + "\"date\""
                    + "},"
                    + " \"data\": { \"type\": \"double\" },"
                    + "\"%s\": { \"type\": \"keyword\"} } } }",
                categoricalField
            );

        RestClient client = ingestClient();
        if (ingestDataSize <= 0) {
            bulkIndexData(data, datasetName(), client, mapping, data.size());
        } else {
            bulkIndexData(data, datasetName(), client, mapping, ingestDataSize);
        }
    }

    protected JsonObject createJsonObject(long timestamp, String component, double dataValue) {
        return createJsonObject(timestamp, component, dataValue, categoricalField);
    }

    protected abstract String genDetector(
        int trainTestSplit,
        long windowDelayMinutes,
        boolean hc,
        ImputationMethod imputation,
        long trainTimeMillis,
        String name
    );

    protected abstract AbstractSyntheticDataTest.GenData genData(
        int trainTestSplit,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE missingMode
    ) throws Exception;

    protected abstract void runTest(
        long firstDataStartTime,
        AbstractSyntheticDataTest.GenData dataGenerated,
        Duration windowDelay,
        String detectorId,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE mode,
        ImputationMethod imputation,
        int numberOfMissingToCheck,
        boolean realTime
    );

    protected abstract double extractFeatureValue(JsonObject source);
}
