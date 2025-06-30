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

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.After;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.google.common.collect.ImmutableMap;

abstract class AbstractMultiTenantAnomalyDetectorRestTestCase extends AnomalyDetectorRestTestCase {

    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";
    private static final int DELETE_RETRY_TIMES = 3;
    protected static final String TIME_FIELD = "timestamp";
    protected static final String VALUE_FIELD = "value";
    private final List<TenantDetectorRef> tenantDetectorsToCleanup = new ArrayList<>();
    private boolean adReenabledDuringCleanup;

    @After
    public void cleanupTenantDetectors() throws Exception {
        if (tenantDetectorsToCleanup.isEmpty()) {
            return;
        }
        adReenabledDuringCleanup = false;

        for (int i = tenantDetectorsToCleanup.size() - 1; i >= 0; i--) {
            cleanupTenantDetector(tenantDetectorsToCleanup.get(i));
        }
        tenantDetectorsToCleanup.clear();
    }

    protected AnomalyDetector createDetectorDefinition(String indexName) throws IOException {
        return createDetectorDefinition(indexName, detectorName("detector"));
    }

    protected AnomalyDetector createDetectorDefinition(String indexName, String detectorName) throws IOException {
        return createDetectorDefinition(indexName, detectorName, true, true, false);
    }

    protected AnomalyDetector createDetectorDefinition(String indexName, String detectorName, boolean createIndex, boolean ingestData)
        throws IOException {
        return createDetectorDefinition(indexName, detectorName, createIndex, ingestData, false);
    }

    protected AnomalyDetector createDateNanosDetectorDefinition(String indexName, String detectorName) throws IOException {
        return createDetectorDefinition(indexName, detectorName, true, true, true);
    }

    protected AnomalyDetector createFeaturelessDetectorDefinition(String indexName, String detectorName, boolean useNullFeatures)
        throws IOException {
        createModelIndex(indexName, true);
        return detectorBuilder(indexName, detectorName, useNullFeatures ? null : List.of()).build();
    }

    private AnomalyDetector createDetectorDefinition(
        String indexName,
        String detectorName,
        boolean createIndex,
        boolean ingestData,
        boolean useDateNanos
    ) throws IOException {
        if (createIndex) {
            createModelIndex(indexName, ingestData, useDateNanos);
        }

        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        return detectorBuilder(indexName, detectorName, List.of(feature)).build();
    }

    protected void createModelIndex(String indexName, boolean ingestData) throws IOException {
        createModelIndex(indexName, ingestData, false);
    }

    protected void createModelIndex(String indexName, boolean ingestData, boolean useDateNanos) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.createIndexWithTimeField(dataClient, indexName, TIME_FIELD, useDateNanos);
            if (ingestData) {
                String testIndexData = String.format(Locale.ROOT, "{\"%s\":1,\"%s\":42}", TIME_FIELD, VALUE_FIELD);
                TestHelpers.ingestDataToIndex(dataClient, indexName, TestHelpers.toHttpEntity(testIndexData));
            }
        }
    }

    protected void createModelHCADIndex(String indexName, Map<String, String> categoryFieldsAndTypes, String documentJson)
        throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            StringBuilder indexMappings = new StringBuilder();
            indexMappings.append("{\"properties\":{");
            for (Map.Entry<String, String> entry : categoryFieldsAndTypes.entrySet()) {
                indexMappings.append("\"").append(entry.getKey()).append("\":{\"type\":\"").append(entry.getValue()).append("\"},");
            }
            indexMappings.append("\"").append(TIME_FIELD).append("\":{\"type\":\"date\"},");
            indexMappings.append("\"").append(VALUE_FIELD).append("\":{\"type\":\"double\"}}}");
            TestHelpers.createEmptyIndex(dataClient, indexName);
            TestHelpers.createIndexMapping(dataClient, indexName, TestHelpers.toHttpEntity(indexMappings.toString()));
            if (documentJson != null) {
                TestHelpers.ingestDataToIndex(dataClient, indexName, TestHelpers.toHttpEntity(documentJson));
            }
        }
    }

    protected void createEmptyModelAnomalyResultIndex(String indexName) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.createEmptyIndex(dataClient, indexName);
            TestHelpers.createIndexMapping(dataClient, indexName, TestHelpers.toHttpEntity(ADIndexManagement.getResultMappings()));
        }
    }

    protected void ingestModelAnomalyResult(String indexName, AnomalyResult anomalyResult) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.ingestDataToIndex(dataClient, indexName, TestHelpers.toHttpEntity(anomalyResult));
        }
    }

    protected void deleteModelIndex(String indexName) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.makeRequest(dataClient, "DELETE", "/" + indexName, ImmutableMap.of(), "", null);
        }
    }

    protected AnomalyResult tenantAnomalyResult(
        String detectorId,
        Map<String, Object> entityAttributes,
        double score,
        double grade,
        long startTimeEpochMillis,
        long endTimeEpochMillis,
        String tenantId
    ) {
        Instant startTime = Instant.ofEpochMilli(startTimeEpochMillis);
        Instant endTime = Instant.ofEpochMilli(endTimeEpochMillis);
        return new AnomalyResult(
            detectorId,
            null,
            score,
            grade,
            0.9,
            List.of(),
            startTime,
            endTime,
            startTime,
            endTime,
            null,
            Optional.ofNullable(entityAttributes == null ? null : Entity.createEntityByReordering(entityAttributes)),
            null,
            CommonValue.NO_SCHEMA_VERSION,
            null,
            null,
            null,
            null,
            null,
            1.0,
            null,
            tenantId
        );
    }

    protected AnomalyDetector createTenantHCADDetector(
        String indexName,
        String detectorName,
        List<String> categoryFields,
        String resultIndex,
        String tenantId
    ) throws IOException {
        AnomalyDetector detector = detectorBuilder(indexName, detectorName)
            .setCategoryFields(categoryFields)
            .setResultIndex(resultIndex)
            .build();
        return createTenantDetector(detector, tenantId);
    }

    protected TestHelpers.AnomalyDetectorBuilder detectorBuilder(String indexName, String detectorName) throws IOException {
        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        return detectorBuilder(List.of(indexName), detectorName, List.of(feature));
    }

    protected TestHelpers.AnomalyDetectorBuilder detectorBuilder(String indexName, String detectorName, List<Feature> features)
        throws IOException {
        return detectorBuilder(List.of(indexName), detectorName, features);
    }

    protected TestHelpers.AnomalyDetectorBuilder detectorBuilder(List<String> indices, String detectorName, List<Feature> features)
        throws IOException {
        return TestHelpers.AnomalyDetectorBuilder
            .newInstance(Math.max(1, features == null ? 0 : features.size()))
            .setName(detectorName)
            .setDescription("Multi-tenant anomaly detector REST IT")
            .setTimeField(TIME_FIELD)
            .setIndices(indices)
            .setFeatureAttributes(features)
            .setFilterQuery(QueryBuilders.matchAllQuery())
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setWindowDelay(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setShingleSize(1)
            .setResultIndex(resultIndexName());
    }

    protected Response createTenantDetectorResponse(AnomalyDetector detector, String tenantId) throws IOException {
        return createTenantDetectorResponse(detector, tenantId, ImmutableMap.of());
    }

    protected Response createTenantDetectorResponse(AnomalyDetector detector, String tenantId, Map<String, String> params)
        throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI,
                params,
                TestHelpers.toHttpEntity(detector),
                tenantHeaders(tenantId)
            );
    }

    protected Response validateTenantDetector(AnomalyDetector detector, String tenantId) throws IOException {
        return validateTenantDetector(detector, "", tenantId);
    }

    protected Response validateTenantDetector(AnomalyDetector detector, String validationType, String tenantId) throws IOException {
        String validationSuffix = validationType == null || validationType.isBlank() ? "" : "/" + validationType;
        AnomalyDetector tenantScopedDetector = withTenantId(detector, tenantId);
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate" + validationSuffix,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(tenantScopedDetector),
                tenantHeaders(tenantId)
            );
    }

    protected Response validateTenantDetector(String payload, String validationType, String tenantId) throws IOException {
        String validationSuffix = validationType == null || validationType.isBlank() ? "" : "/" + validationType;
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate" + validationSuffix,
                ImmutableMap.of(),
                payload,
                tenantHeaders(tenantId)
            );
    }

    protected AnomalyDetector createTenantDetector(AnomalyDetector detector, String tenantId) throws IOException {
        AnomalyDetector createdDetector = createAnomalyDetector(detector, true, client(), tenantHeaders(tenantId));
        tenantDetectorsToCleanup.add(new TenantDetectorRef(createdDetector.getId(), tenantId));
        return createdDetector;
    }

    protected AnomalyDetector getTenantDetector(String detectorId, String tenantId) throws IOException {
        return getConfig(detectorId, tenantHeaders(tenantId), client());
    }

    protected ToXContentObject[] getTenantDetector(String detectorId, boolean returnJob, String tenantId) throws IOException {
        return getConfig(detectorId, tenantJsonHeaders(tenantId), returnJob, false, client());
    }

    protected boolean modelAliasExists(String alias) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.makeRequest(dataClient, "GET", "/_alias/" + alias, ImmutableMap.of(), "", null);
            return true;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    protected boolean modelPipelineExists(String pipelineId) throws IOException {
        try (RestClient dataClient = buildClient(restClientSettings(), modelHosts())) {
            TestHelpers.makeRequest(dataClient, "GET", "/_ingest/pipeline/" + pipelineId, ImmutableMap.of(), "", null);
            return true;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    protected Response updateTenantDetector(String detectorId, AnomalyDetector detector, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                tenantHeaders(tenantId)
            );
    }

    protected Response deleteTenantDetector(String detectorId, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId,
                ImmutableMap.of(),
                "",
                tenantHeaders(tenantId)
            );
    }

    protected Response startTenantDetector(String detectorId, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start",
                ImmutableMap.of(),
                "",
                tenantHeaders(tenantId)
            );
    }

    protected Response stopTenantDetector(String detectorId, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_stop",
                ImmutableMap.of(),
                "",
                tenantHeaders(tenantId)
            );
    }

    protected Response previewTenantDetector(AnomalyDetectorExecutionInput input, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(input),
                tenantHeaders(tenantId)
            );
    }

    protected AnomalyDetectorExecutionInput tenantExecutionInput(
        String detectorId,
        Instant periodStart,
        Instant periodEnd,
        AnomalyDetector detector,
        String tenantId
    ) {
        return new AnomalyDetectorExecutionInput(
            detectorId,
            periodStart,
            periodEnd,
            detector == null ? null : withTenantId(detector, tenantId)
        );
    }

    protected Response searchTenantDetectors(String queryJson, String tenantId) throws IOException {
        return searchTenantDetectors(new StringEntity(queryJson), tenantId);
    }

    protected Response searchTenantDetectors(HttpEntity queryEntity, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                ImmutableMap.of(),
                queryEntity,
                tenantHeaders(tenantId)
            );
    }

    protected Response getTenantDetectorCount(String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + RestHandlerUtils.COUNT,
                ImmutableMap.of(),
                "",
                tenantKibanaHeaders(tenantId)
            );
    }

    protected Response getTenantDetectorMatch(String name, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + RestHandlerUtils.MATCH,
                ImmutableMap.of("name", name),
                "",
                tenantKibanaHeaders(tenantId)
            );
    }

    protected Response getTenantDetectorProfile(String detectorId, String tenantId) throws IOException {
        return getTenantDetectorProfile(detectorId, false, "", tenantId);
    }

    protected Response getTenantDetectorProfile(String detectorId, boolean all, String customizedProfile, String tenantId)
        throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/" + RestHandlerUtils.PROFILE + customizedProfile + "?_all=" + all,
                null,
                "",
                tenantKibanaHeaders(tenantId)
            );
    }

    protected Response searchTopTenantAnomalyResults(String detectorId, boolean historical, String bodyAsJsonString, String tenantId)
        throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI
                    + "/"
                    + detectorId
                    + "/"
                    + RestHandlerUtils.RESULTS
                    + "/"
                    + RestHandlerUtils.TOP_ANOMALIES,
                ImmutableMap.of("historical", String.valueOf(historical)),
                TestHelpers.toHttpEntity(bodyAsJsonString),
                tenantHeaders(tenantId)
            );
    }

    protected AnomalyDetector copyDetector(AnomalyDetector detector, String detectorId, Long version, String name, String description) {
        return copyDetector(
            detector,
            detectorId,
            version,
            name,
            description,
            detector.getCategoryFields(),
            detector.getFlattenResultIndexMapping()
        );
    }

    protected AnomalyDetector copyDetector(
        AnomalyDetector detector,
        String detectorId,
        Long version,
        String name,
        String description,
        List<String> categoryFields,
        Boolean flattenResultIndexMapping
    ) {
        return copyDetector(
            detector,
            detectorId,
            version,
            name,
            description,
            categoryFields,
            flattenResultIndexMapping,
            detector.getTenantId()
        );
    }

    protected AnomalyDetector copyDetector(
        AnomalyDetector detector,
        String detectorId,
        Long version,
        String name,
        String description,
        List<String> categoryFields,
        Boolean flattenResultIndexMapping,
        String tenantId
    ) {
        return new AnomalyDetector(
            detectorId,
            version,
            name,
            description,
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            categoryFields,
            detector.getUser(),
            detector.getCustomResultIndexOrAlias(),
            detector.getImputationOption(),
            detector.getRecencyEmphasis(),
            detector.getSeasonIntervals(),
            detector.getHistoryIntervals(),
            detector.getRules(),
            detector.getCustomResultIndexMinSize(),
            detector.getCustomResultIndexMinAge(),
            detector.getCustomResultIndexTTL(),
            flattenResultIndexMapping,
            detector.getLastBreakingUIChangeTime(),
            detector.getFrequency(),
            detector.getAutoCreated(),
            tenantId
        );
    }

    protected AnomalyDetector withTenantId(AnomalyDetector detector, String tenantId) {
        return copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            detector.getDescription(),
            detector.getCategoryFields(),
            detector.getFlattenResultIndexMapping(),
            tenantId
        );
    }

    protected String detectorName(String suffix) {
        return "mt-" + suffix + "-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
    }

    protected String indexName(String suffix) {
        return "mt-" + suffix + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    protected String tenantId(String suffix) {
        return "tenant-" + suffix + "-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
    }

    protected String resultIndexName() {
        return ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
    }

    protected String validationPayload(String tenantId, String bodyFields) {
        return "{" + "\"tenant_id\":\"" + tenantId + "\"," + "\"result_index\":\"" + resultIndexName() + "\"," + bodyFields + "}";
    }

    protected List<Header> tenantHeaders(String tenantId) {
        return List
            .of(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"), new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId));
    }

    protected List<Header> tenantJsonHeaders(String tenantId) {
        return List
            .of(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"), new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId));
    }

    protected List<Header> tenantKibanaHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"),
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId)
            );
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

    private void cleanupTenantDetector(TenantDetectorRef detectorRef) throws Exception {
        stopTenantDetectorIfPresent(detectorRef);
        for (int attempt = 0; attempt < DELETE_RETRY_TIMES; attempt++) {
            if (deleteTenantDetectorIfPresent(detectorRef)) {
                return;
            }
            stopTenantDetectorIfPresent(detectorRef);
            Thread.sleep(1000L);
        }
        LOG.warn("Failed to clean up multi-tenant detector {}", detectorRef.detectorId);
    }

    private void stopTenantDetectorIfPresent(TenantDetectorRef detectorRef) throws Exception {
        try {
            stopTenantDetector(detectorRef.detectorId, detectorRef.tenantId);
        } catch (ResponseException e) {
            if (enableAdForCleanupIfNeeded(e)) {
                stopTenantDetectorIfPresent(detectorRef);
                return;
            }
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                LOG.warn("Failed to stop multi-tenant detector {} during cleanup", detectorRef.detectorId, e);
            }
        }
    }

    private boolean deleteTenantDetectorIfPresent(TenantDetectorRef detectorRef) throws Exception {
        try {
            deleteTenantDetector(detectorRef.detectorId, detectorRef.tenantId);
            return true;
        } catch (ResponseException e) {
            if (enableAdForCleanupIfNeeded(e)) {
                return deleteTenantDetectorIfPresent(detectorRef);
            }
            int statusCode = e.getResponse().getStatusLine().getStatusCode();
            if (statusCode == 404) {
                return true;
            }
            if (statusCode == 400 && e.getMessage().contains("Job is running")) {
                return false;
            }
            LOG.warn("Failed to delete multi-tenant detector {} during cleanup", detectorRef.detectorId, e);
            return false;
        }
    }

    private boolean enableAdForCleanupIfNeeded(ResponseException e) throws Exception {
        if (!adReenabledDuringCleanup && e.getMessage().contains(ADCommonMessages.DISABLED_ERR_MSG)) {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
            adReenabledDuringCleanup = true;
            return true;
        }
        return false;
    }

    private static final class TenantDetectorRef {
        private final String detectorId;
        private final String tenantId;

        private TenantDetectorRef(String detectorId, String tenantId) {
            this.detectorId = detectorId;
            this.tenantId = tenantId;
        }
    }
}
