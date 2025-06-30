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

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.DUPLICATE_DETECTOR_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.tests.util.TimeUnits;
import org.awaitility.Awaitility;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public class MultiTenantAnomalyDetectorRestApiIT extends AbstractMultiTenantAnomalyDetectorRestTestCase {

    public void testCreateAnomalyDetectorWithNotExistingIndices() throws Exception {
        String tenantId = tenantId("missing-index");
        AnomalyDetector detector = createDetectorDefinition(indexName("missing-index"), detectorName("missing-index"), false, false);

        ResponseException exception = expectThrows(ResponseException.class, () -> createTenantDetectorResponse(detector, tenantId));
        assertThat(exception.getMessage(), containsString("Fail to get the index mapping of ["));
    }

    public void testCreateAnomalyDetectorAllowsSameNameAcrossTenants() throws Exception {
        String sharedName = detectorName("shared-name");
        String firstTenant = tenantId("create-a");
        String secondTenant = tenantId("create-b");

        AnomalyDetector firstDetector = createTenantDetector(createDetectorDefinition(indexName("create-a"), sharedName), firstTenant);
        AnomalyDetector secondDetector = createTenantDetector(createDetectorDefinition(indexName("create-b"), sharedName), secondTenant);

        assertEquals(sharedName, firstDetector.getName());
        assertEquals(sharedName, secondDetector.getName());
        assertNotEquals(firstDetector.getId(), secondDetector.getId());
    }

    public void testCreateAnomalyDetectorWithDuplicateNameInSameTenant() throws Exception {
        String tenantId = tenantId("duplicate-create");
        String detectorName = detectorName("duplicate-create");
        AnomalyDetector existingDetector = createTenantDetector(
            createDetectorDefinition(indexName("duplicate-create-a"), detectorName),
            tenantId
        );
        awaitTenantDetectorNameMatch(detectorName, tenantId, true);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createTenantDetectorResponse(createDetectorDefinition(indexName("duplicate-create-b"), detectorName), tenantId)
        );
        assertThat(exception.getMessage(), containsString(duplicateDetectorMessage(detectorName, existingDetector.getId())));
    }

    public void testCreateAnomalyDetectorIgnoresSuppliedDetectorId() throws Exception {
        String tenantId = tenantId("supplied-id");
        String suppliedDetectorId = "__provided__";
        AnomalyDetector detector = createDetectorDefinition(indexName("supplied-id"), detectorName("supplied-id"));

        Response response = createTenantDetectorResponse(
            detector,
            tenantId,
            ImmutableMap.of(RestHandlerUtils.DETECTOR_ID, suppliedDetectorId)
        );
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String detectorId = (String) responseMap.get("_id");
        trackTenantDetector(detectorId, tenantId);
        assertNotEquals(suppliedDetectorId, detectorId);
    }

    public void testCreateAnomalyDetectorWithDateNanos() throws Exception {
        String tenantId = tenantId("date-nanos");
        AnomalyDetector detector = createDateNanosDetectorDefinition(indexName("date-nanos"), detectorName("date-nanos"));

        AnomalyDetector createdDetector = createTenantDetector(detector, tenantId);
        assertNotNull(createdDetector.getId());
    }

    public void testCreateAnomalyDetectorWhenADDisabled() throws Exception {
        String tenantId = tenantId("create-disabled");
        AnomalyDetector detector = createDetectorDefinition(indexName("create-disabled"), detectorName("create-disabled"));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> createTenantDetectorResponse(detector, tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }
    }

    public void testGetAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("get");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("get")), tenantId);

        AnomalyDetector fetched = getTenantDetector(detector.getId(), tenantId);
        assertEquals(detector.getId(), fetched.getId());
        assertEquals(detector.getName(), fetched.getName());
    }

    public void testGetNotExistingAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("get-missing");
        createTenantDetector(createDetectorDefinition(indexName("get-missing")), tenantId);

        expectThrows(ResponseException.class, () -> getTenantDetector(randomAlphaOfLength(8), tenantId));
    }

    public void testGetAnomalyDetectorWhenADDisabled() throws Exception {
        String tenantId = tenantId("get-disabled");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("get-disabled")), tenantId);

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> getTenantDetector(detector.getId(), tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }
    }

    public void testGetAnomalyDetectorWithAdJobAndTenantHeader() throws Exception {
        String tenantId = tenantId("get-job");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("get-job")), tenantId);

        Response startResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));

        ToXContentObject[] results = getTenantDetector(detector.getId(), true, tenantId);
        assertEquals(detector.getId(), ((AnomalyDetector) results[0]).getId());
        assertEquals(detector.getId(), ((Job) results[1]).getName());
        assertTrue(((Job) results[1]).isEnabled());

        results = getTenantDetector(detector.getId(), false, tenantId);
        assertEquals(detector.getId(), ((AnomalyDetector) results[0]).getId());
        assertNull(results[1]);
    }

    public void testUpdateAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("update");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("update")), tenantId);

        String newDescription = randomAlphaOfLength(12);
        AnomalyDetector updatedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            newDescription
        );

        Response updateResponse = updateTenantDetector(detector.getId(), updatedDetector, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(updateResponse));
        assertEquals(detector.getId(), entityAsMap(updateResponse).get("_id"));
        assertEquals(newDescription, getTenantDetector(detector.getId(), tenantId).getDescription());
    }

    public void testUpdateAnomalyDetectorNameToNewWithTenantHeader() throws Exception {
        String tenantId = tenantId("rename");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("rename")), tenantId);

        String newName = detectorName("renamed");
        AnomalyDetector renamedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            newName,
            detector.getDescription()
        );

        Response updateResponse = updateTenantDetector(detector.getId(), renamedDetector, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(updateResponse));
        assertEquals(newName, getTenantDetector(detector.getId(), tenantId).getName());
    }

    public void testUpdateAnomalyDetectorNameToExistingInSameTenant() throws Exception {
        String tenantId = tenantId("rename-duplicate");
        AnomalyDetector detectorToRename = createTenantDetector(createDetectorDefinition(indexName("rename-duplicate-a")), tenantId);
        AnomalyDetector existingDetector = createTenantDetector(createDetectorDefinition(indexName("rename-duplicate-b")), tenantId);
        awaitTenantDetectorNameMatch(existingDetector.getName(), tenantId, true);

        AnomalyDetector renamedDetector = copyDetector(
            detectorToRename,
            detectorToRename.getId(),
            detectorToRename.getVersion(),
            existingDetector.getName(),
            detectorToRename.getDescription()
        );

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> updateTenantDetector(detectorToRename.getId(), renamedDetector, tenantId)
            );
            assertThat(
                exception.getMessage(),
                containsString(duplicateDetectorMessage(existingDetector.getName(), existingDetector.getId()))
            );
        });
    }

    public void testUpdateAnomalyDetectorCategoryFieldRejectedWithTenantHeader() throws Exception {
        String tenantId = tenantId("category");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("category")), tenantId);

        AnomalyDetector updatedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            detector.getDescription(),
            ImmutableList.of(randomAlphaOfLength(5)),
            detector.getFlattenResultIndexMapping()
        );

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> updateTenantDetector(detector.getId(), updatedDetector, tenantId)
        );
        assertThat(exception.getMessage(), containsString(CommonMessages.CAN_NOT_CHANGE_CATEGORY_FIELD));
    }

    public void testCreateAnomalyDetectorWithFlattenedResultIndex() throws Exception {
        String tenantId = tenantId("flattened");
        AnomalyDetector detector = createDetectorDefinition(indexName("flattened"), detectorName("flattened"));
        AnomalyDetector flattenedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            detector.getDescription(),
            detector.getCategoryFields(),
            true
        );

        if (isAossDataPlane()) {
            ResponseException exception = expectThrows(ResponseException.class, () -> createTenantDetector(flattenedDetector, tenantId));
            assertThat(
                exception.getMessage(),
                containsString(AbstractTimeSeriesActionHandler.AOSS_FLATTEN_CUSTOM_RESULT_INDEX_UNSUPPORTED)
            );
            return;
        }

        AnomalyDetector createdDetector = createTenantDetector(flattenedDetector, tenantId);
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return modelAliasExists(createdDetector.getFlattenResultIndexAlias());
            } catch (Exception e) {
                return false;
            }
        });
    }

    public void testUpdateAnomalyDetectorFlattenResultIndexFieldWithTenantHeader() throws Exception {
        String tenantId = tenantId("flatten-update");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("flatten-update")), tenantId);
        AnomalyDetector flattenedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            detector.getDescription(),
            detector.getCategoryFields(),
            true
        );
        String pipelineId = "flatten_result_index_ingest_pipeline_" + detector.getName().toLowerCase(Locale.ROOT);

        if (isAossDataPlane()) {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> updateTenantDetector(detector.getId(), flattenedDetector, tenantId)
            );
            assertThat(
                exception.getMessage(),
                containsString(AbstractTimeSeriesActionHandler.AOSS_FLATTEN_CUSTOM_RESULT_INDEX_UNSUPPORTED)
            );
            return;
        }

        Response updateResponse = updateTenantDetector(detector.getId(), flattenedDetector, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(updateResponse));

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return modelPipelineExists(pipelineId);
            } catch (Exception e) {
                return false;
            }
        });
    }

    public void testUpdateAnomalyDetectorDisableFlattenResultIndexShouldDeletePipelineWithTenantHeader() throws Exception {
        String tenantId = tenantId("flatten-off");
        AnomalyDetector detectorDefinition = createDetectorDefinition(indexName("flatten-off"), detectorName("flatten-off"));
        AnomalyDetector flattenedDetector = copyDetector(
            detectorDefinition,
            detectorDefinition.getId(),
            detectorDefinition.getVersion(),
            detectorDefinition.getName(),
            detectorDefinition.getDescription(),
            detectorDefinition.getCategoryFields(),
            true
        );
        if (isAossDataPlane()) {
            ResponseException exception = expectThrows(ResponseException.class, () -> createTenantDetector(flattenedDetector, tenantId));
            assertThat(
                exception.getMessage(),
                containsString(AbstractTimeSeriesActionHandler.AOSS_FLATTEN_CUSTOM_RESULT_INDEX_UNSUPPORTED)
            );
            return;
        }

        AnomalyDetector detector = createTenantDetector(flattenedDetector, tenantId);
        String pipelineId = "flatten_result_index_ingest_pipeline_" + detector.getName().toLowerCase(Locale.ROOT);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return modelPipelineExists(pipelineId);
            } catch (Exception e) {
                return false;
            }
        });

        AnomalyDetector latestDetector = getTenantDetector(detector.getId(), tenantId);
        AnomalyDetector nonFlattenedDetector = copyDetector(
            latestDetector,
            latestDetector.getId(),
            latestDetector.getVersion(),
            latestDetector.getName(),
            latestDetector.getDescription(),
            latestDetector.getCategoryFields(),
            false
        );

        Response updateResponse = updateTenantDetector(latestDetector.getId(), nonFlattenedDetector, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(updateResponse));

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return modelPipelineExists(pipelineId) == false;
            } catch (Exception e) {
                return false;
            }
        });
    }

    public void testValidateAnomalyDetectorWithNoIssue() throws Exception {
        String tenantId = tenantId("validate-no-issue");
        AnomalyDetector detector = createDetectorDefinition(indexName("validate-no-issue"), detectorName("validate-no-issue"));

        Response response = validateTenantDetector(detector, "detector", tenantId);
        assertTrue(entityAsMap(response).isEmpty());
    }

    public void testValidateAnomalyDetectorWithDuplicateNameInSameTenant() throws Exception {
        String tenantId = tenantId("validate-duplicate");
        String detectorName = detectorName("validate-duplicate");
        AnomalyDetector existingDetector = createTenantDetector(
            createDetectorDefinition(indexName("validate-duplicate-a"), detectorName),
            tenantId
        );
        awaitTenantDetectorNameMatch(detectorName, tenantId, true);
        AnomalyDetector duplicateNameDetector = createDetectorDefinition(indexName("validate-duplicate-b"), detectorName);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response response = validateTenantDetector(duplicateNameDetector, "detector", tenantId);
            Map<String, Map<String, String>> validationMessages = detectorValidationMessages(response);
            assertNotNull(validationMessages);
            assertNotNull(validationMessages.get("name"));
            assertEquals(duplicateDetectorMessage(detectorName, existingDetector.getId()), validationMessages.get("name").get("message"));
        });
    }

    public void testValidateAnomalyDetectorHonorsDynamicMaxHCDetectorSettingPerTenant() throws Exception {
        String tenantId = tenantId("validate-hc-limit");
        String otherTenantId = tenantId("validate-hc-limit-other");
        String expectedErrorMessage = String.format(Locale.ROOT, EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG, 1);

        createTenantDetector(createHCDetectorDefinition(indexName("validate-hc-limit-other-a")), otherTenantId);

        try {
            updateClusterSettings(AnomalyDetectorSettings.AD_MAX_HC_ANOMALY_DETECTORS.getKey(), 1);

            AnomalyDetector otherTenantCandidate = createHCDetectorDefinition(indexName("validate-hc-limit-other-b"));
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                assertMaxHCDetectorValidationIssue(otherTenantCandidate, otherTenantId, expectedErrorMessage);
            });

            AnomalyDetector firstTenantDetector = createHCDetectorDefinition(indexName("validate-hc-limit-a"));
            Response validationResponse = validateTenantDetector(firstTenantDetector, "detector", tenantId);
            assertTrue(entityAsMap(validationResponse).isEmpty());
            createTenantDetector(firstTenantDetector, tenantId);

            AnomalyDetector sameTenantCandidate = createHCDetectorDefinition(indexName("validate-hc-limit-b"));
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                assertMaxHCDetectorValidationIssue(sameTenantCandidate, tenantId, expectedErrorMessage);
            });
        } finally {
            updateClusterSettings(AnomalyDetectorSettings.AD_MAX_HC_ANOMALY_DETECTORS.getKey(), 1000);
        }
    }

    public void testValidateAnomalyDetectorOnWrongValidationType() throws Exception {
        String tenantId = tenantId("validate-type");
        AnomalyDetector detector = createDetectorDefinition(indexName("validate-type"), detectorName("validate-type"));

        ResponseException exception = expectThrows(ResponseException.class, () -> validateTenantDetector(detector, "models", tenantId));
        assertThat(exception.getMessage(), containsString(CommonMessages.NOT_EXISTENT_VALIDATION_TYPE));
    }

    public void testValidateAnomalyDetectorWithNoTimeField() throws Exception {
        String tenantId = tenantId("validate-no-time");
        String indexName = indexName("validate-no-time");

        createModelIndex(indexName, true);
        Response response = validateTenantDetector(
            validationPayload(
                tenantId,
                "\"name\":\""
                    + detectorName("validate-no-time")
                    + "\",\"description\":\"\",\"indices\":[\""
                    + indexName
                    + "\"],\"feature_attributes\":[{\"feature_name\":\"test\",\"feature_enabled\":true,\"aggregation_query\":{\"test\":{\"sum\":{\"field\":\""
                    + VALUE_FIELD
                    + "\"}}}}],\"filter_query\":{},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}"
            ),
            "detector",
            tenantId
        );

        assertEquals(CommonMessages.NULL_TIME_FIELD, detectorValidationMessages(response).get("time_field").get("message"));
    }

    public void testValidateAnomalyDetectorWithMultipleIndices() throws Exception {
        String tenantId = tenantId("validate-multi");
        String firstIndex = indexName("validate-multi-ok-a");
        String secondIndex = indexName("validate-multi-ok-b");

        createModelIndex(firstIndex, true);
        createModelIndex(secondIndex, true);
        Response response = validateTenantDetector(
            validationPayload(
                tenantId,
                "\"name\":\""
                    + detectorName("validate-multi")
                    + "\",\"description\":\"Test detector\",\"time_field\":\""
                    + TIME_FIELD
                    + "\",\"indices\":[\""
                    + firstIndex
                    + "\",\""
                    + secondIndex
                    + "\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\"feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\""
                    + VALUE_FIELD
                    + "\"}}}}],\"filter_query\":{\"match_all\":{}},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},\"shingle_size\":8"
            ),
            "",
            tenantId
        );

        assertTrue(entityAsMap(response).isEmpty());
    }

    public void testValidateAnomalyDetectorWithIncorrectShingleSize() throws Exception {
        String tenantId = tenantId("validate-shingle");
        String indexName = indexName("validate-shingle");

        createModelIndex(indexName, true);
        Response response = validateTenantDetector(
            validationPayload(
                tenantId,
                "\"name\":\""
                    + detectorName("validate-shingle")
                    + "\",\"description\":\"Test detector\",\"time_field\":\""
                    + TIME_FIELD
                    + "\",\"indices\":[\""
                    + indexName
                    + "\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\"feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\""
                    + VALUE_FIELD
                    + "\"}}}}],\"filter_query\":{\"match_all\":{}},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},\"shingle_size\":2000"
            ),
            "",
            tenantId
        );

        String errorMessage = "Suggested shingle size must be between 1 and " + TimeSeriesSettings.MAX_SHINGLE_SIZE + ". Got 2000.";
        assertEquals(errorMessage, detectorValidationMessages(response).get("shingle_size").get("message"));
    }

    public void testValidateAnomalyDetectorWithInvalidName() throws Exception {
        String tenantId = tenantId("validate-name");
        String indexName = indexName("validate-name");

        createModelIndex(indexName, true);
        Response response = validateTenantDetector(
            validationPayload(
                tenantId,
                "\"name\":\"#@$3\",\"description\":\"\",\"time_field\":\""
                    + TIME_FIELD
                    + "\",\"indices\":[\""
                    + indexName
                    + "\"],\"feature_attributes\":[{\"feature_name\":\"test\",\"feature_enabled\":true,\"aggregation_query\":{\"test\":{\"sum\":{\"field\":\""
                    + VALUE_FIELD
                    + "\"}}}}],\"filter_query\":{},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}"
            ),
            "detector",
            tenantId
        );

        assertEquals(CommonMessages.INVALID_NAME, detectorValidationMessages(response).get("name").get("message"));
    }

    public void testValidateAnomalyDetectorWithFeatureQueryReturningNoData() throws Exception {
        String tenantId = tenantId("validate-empty-feature");
        String indexName = indexName("validate-empty-feature");
        Feature emptyFeature = TestHelpers.randomFeature("f-empty", "cpu", "avg", true);

        createModelIndex(indexName, true);
        AnomalyDetector detector = detectorBuilder(indexName, detectorName("validate-empty-feature"), ImmutableList.of(emptyFeature))
            .build();

        Response response = validateTenantDetector(detector, "detector", tenantId);
        assertEquals(
            CommonMessages.FEATURE_WITH_EMPTY_DATA_MSG + "f-empty",
            detectorValidationMessages(response).get("feature_attributes").get("message")
        );
    }

    public void testValidateAnomalyDetectorWithWrongCategoryField() throws Exception {
        String tenantId = tenantId("validate-category");
        String indexName = indexName("validate-category");
        String categoryField = "host.keyword";

        createModelIndex(indexName, true);
        AnomalyDetector detector = detectorBuilder(indexName, detectorName("validate-category"))
            .setCategoryFields(ImmutableList.of(categoryField))
            .build();

        Response response = validateTenantDetector(detector, "detector", tenantId);
        String errorMessage = String
            .format(Locale.ROOT, AbstractTimeSeriesActionHandler.CATEGORY_NOT_FOUND_ERR_MSG, categoryField, "[" + indexName + "]");
        assertEquals(errorMessage, detectorValidationMessages(response).get("category_field").get("message"));
    }

    public void testSearchAnomalyDetectorWhenADDisabled() throws Exception {
        String tenantId = tenantId("search-disabled");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("search-disabled")), tenantId);

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> searchTenantDetectors(detectorNameQuery(detector.getName()), tenantId)
            );
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }
    }

    public void testPreviewAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("preview");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("preview")), tenantId);
        AnomalyDetectorExecutionInput input = tenantExecutionInput(
            detector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null,
            tenantId
        );

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> previewTenantDetector(input, tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }

        Response response = previewTenantDetector(input, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testPreviewAnomalyDetectorWhichNotExistWithTenantHeader() throws Exception {
        String tenantId = tenantId("preview-missing");
        createTenantDetector(createDetectorDefinition(indexName("preview-missing")), tenantId);

        AnomalyDetectorExecutionInput input = tenantExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null,
            tenantId
        );
        expectThrows(ResponseException.class, () -> previewTenantDetector(input, tenantId));
    }

    public void testExecuteAnomalyDetectorWithNullDetectorIdAndTenantHeader() throws Exception {
        String tenantId = tenantId("preview-null-id");
        AnomalyDetectorExecutionInput input = tenantExecutionInput(
            null,
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null,
            tenantId
        );

        expectThrows(ResponseException.class, () -> previewTenantDetector(input, tenantId));
    }

    public void testPreviewAnomalyDetectorWithDetectorAndTenantHeader() throws Exception {
        String tenantId = tenantId("preview-detector");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("preview-detector")), tenantId);
        AnomalyDetectorExecutionInput input = tenantExecutionInput(
            detector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            detector,
            tenantId
        );

        Response response = previewTenantDetector(input, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testPreviewAnomalyDetectorWithDetectorAndNoFeaturesAndTenantHeader() throws Exception {
        String tenantId = tenantId("preview-no-features");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("preview-no-features")), tenantId);
        AnomalyDetectorExecutionInput input = tenantExecutionInput(
            detector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            createFeaturelessDetectorDefinition(indexName("preview-input-no-features"), detectorName("preview-input-no-features"), false),
            tenantId
        );

        ResponseException exception = expectThrows(ResponseException.class, () -> previewTenantDetector(input, tenantId));
        assertThat(exception.getMessage(), containsString("Can't preview detector without feature"));
    }

    public void testSearchTopAnomalyResultsWithInvalidInputsAndTenantHeader() throws Exception {
        String tenantId = tenantId("top-invalid");
        String indexName = indexName("top-invalid");
        String resultIndex = resultIndexName();
        Map<String, String> categoryFieldsAndTypes = ImmutableMap.of("keyword-field", "keyword", "ip-field", "ip");

        createModelHCADIndex(
            indexName,
            categoryFieldsAndTypes,
            "{\"keyword-field\":\"field-1\",\"ip-field\":\"1.2.3.4\",\"timestamp\":1,\"value\":42}"
        );
        AnomalyDetector detector = createTenantHCADDetector(
            indexName,
            detectorName("top-invalid"),
            ImmutableList.of("keyword-field", "ip-field"),
            resultIndex,
            tenantId
        );

        Exception missingStartTimeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId(), false, "{\"end_time_ms\":2}", tenantId)
        );
        assertThat(
            missingStartTimeException.getMessage(),
            containsString("Must set both start time and end time with epoch of milliseconds")
        );

        Exception missingEndTimeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1}", tenantId)
        );
        assertThat(
            missingEndTimeException.getMessage(),
            containsString("Must set both start time and end time with epoch of milliseconds")
        );

        Exception invalidTimeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":2, \"end_time_ms\":1}", tenantId)
        );
        assertThat(invalidTimeException.getMessage(), containsString("Start time should be before end time"));

        Exception invalidDetectorIdException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId() + "-invalid", false, "{\"start_time_ms\":1, \"end_time_ms\":2}", tenantId)
        );
        assertTrue(
            invalidDetectorIdException.getMessage().contains("Can't find config with id")
                || invalidDetectorIdException.getMessage().contains("No anomaly detector found with ID")
        );

        Exception invalidOrderException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2, \"order\":\"invalid-order\"}",
                tenantId
            )
        );
        assertThat(invalidOrderException.getMessage(), containsString("Ordering by invalid-order is not a valid option"));

        Exception negativeSizeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":-1}", tenantId)
        );
        assertThat(negativeSizeException.getMessage(), containsString("Size must be a positive integer"));

        Exception zeroSizeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":0}", tenantId)
        );
        assertThat(zeroSizeException.getMessage(), containsString("Size must be a positive integer"));

        Exception tooLargeSizeException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":9999999}",
                tenantId
            )
        );
        assertThat(tooLargeSizeException.getMessage(), containsString("Size cannot exceed"));

        Exception invalidCategoryFieldsException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2, \"category_field\":[\"invalid-field\"]}",
                tenantId
            )
        );
        assertThat(
            invalidCategoryFieldsException.getMessage(),
            containsString("Category field invalid-field doesn't exist for detector ID " + detector.getId())
        );

        AnomalyDetector detectorWithNoCategoryFields = createTenantDetector(
            detectorBuilder(indexName, detectorName("top-no-category"))
                .setCategoryFields(ImmutableList.of())
                .setResultIndex(resultIndexName())
                .build(),
            tenantId
        );
        Exception noCategoryFieldsException = expectThrows(
            IOException.class,
            () -> searchTopTenantAnomalyResults(
                detectorWithNoCategoryFields.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2}",
                tenantId
            )
        );
        assertThat(
            noCategoryFieldsException.getMessage(),
            containsString("No category fields found for detector ID " + detectorWithNoCategoryFields.getId())
        );
    }

    public void testSearchTopAnomalyResultsOnEmptyCustomResultIndexWithTenantHeader() throws Exception {
        String tenantId = tenantId("top-custom");
        String indexName = indexName("top-custom");
        String resultIndex = resultIndexName();
        Map<String, String> categoryFieldsAndTypes = ImmutableMap.of("keyword-field", "keyword", "ip-field", "ip");

        createModelHCADIndex(
            indexName,
            categoryFieldsAndTypes,
            "{\"keyword-field\":\"field-1\",\"ip-field\":\"1.2.3.4\",\"timestamp\":1,\"value\":42}"
        );
        AnomalyDetector detector = createTenantHCADDetector(
            indexName,
            detectorName("top-custom"),
            ImmutableList.of("keyword-field", "ip-field"),
            resultIndex,
            tenantId
        );

        Response response = searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":0, \"end_time_ms\":10}", tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        assertEquals(0, topAnomalyBuckets(response).size());
    }

    public void testSearchTopAnomalyResultsOnNonExistentCustomResultIndexWithTenantHeader() throws Exception {
        String tenantId = tenantId("top-missing-result");
        String indexName = indexName("top-missing-result");
        String resultIndex = resultIndexName();
        Map<String, String> categoryFieldsAndTypes = ImmutableMap.of("keyword-field", "keyword", "ip-field", "ip");

        createModelHCADIndex(
            indexName,
            categoryFieldsAndTypes,
            "{\"keyword-field\":\"field-1\",\"ip-field\":\"1.2.3.4\",\"timestamp\":1,\"value\":42}"
        );
        AnomalyDetector detector = createTenantHCADDetector(
            indexName,
            detectorName("top-missing-result"),
            ImmutableList.of("keyword-field", "ip-field"),
            resultIndex,
            tenantId
        );
        deleteModelIndex(resultIndex + "-history-*");

        Response response = searchTopTenantAnomalyResults(detector.getId(), false, "{\"start_time_ms\":0, \"end_time_ms\":10}", tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        assertEquals(0, topAnomalyBuckets(response).size());
    }

    public void testSearchTopAnomalyResultsWithCustomResultIndexAndTenantHeader() throws Exception {
        String tenantId = tenantId("top-custom-populated");
        String indexName = indexName("top-custom-populated");
        String resultIndex = resultIndexName();
        Map<String, String> categoryFieldsAndTypes = ImmutableMap.of("keyword-field", "keyword", "ip-field", "ip");

        createModelHCADIndex(
            indexName,
            categoryFieldsAndTypes,
            "{\"keyword-field\":\"field-1\",\"ip-field\":\"1.2.3.4\",\"timestamp\":1,\"value\":42}"
        );
        AnomalyDetector detector = createTenantHCADDetector(
            indexName,
            detectorName("top-custom-populated"),
            ImmutableList.of("keyword-field", "ip-field"),
            resultIndex,
            tenantId
        );

        ingestModelAnomalyResult(
            resultIndex,
            tenantAnomalyResult(
                detector.getId(),
                ImmutableMap.of("keyword-field", "field-1", "ip-field", "1.2.3.4"),
                0.5,
                0.8,
                0L,
                10L,
                tenantId
            )
        );

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response response = searchTopTenantAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":0, \"end_time_ms\":10}",
                tenantId
            );
            assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

            List<Map<String, Object>> buckets = topAnomalyBuckets(response);
            assertEquals(1, buckets.size());
            assertEquals("field-1", bucketKey(buckets.get(0)).get("keyword-field"));
            assertEquals("1.2.3.4", bucketKey(buckets.get(0)).get("ip-field"));
        });
    }

    public void testSearchAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("search");
        String otherTenantId = tenantId("search-other");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("search")), tenantId);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response searchResponse = searchTenantDetectors(detectorIdQuery(detector.getId()), tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(searchResponse));

            List<Map<String, Object>> hits = searchHits(searchResponse);
            assertEquals(1, hits.size());
            assertEquals(detector.getId(), hits.get(0).get("_id"));
        });

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response searchResponse = searchTenantDetectors(detectorIdQuery(detector.getId()), otherTenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(searchResponse));
            assertEquals(0, searchHits(searchResponse).size());
        });
    }

    public void testSearchAnomalyDetectorCountNoMatchForNewTenant() throws Exception {
        String tenantId = tenantId("count-empty");

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response countResponse = getTenantDetectorCount(tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(countResponse));
            assertEquals(0L, detectorCount(countResponse));
        });
    }

    public void testSearchAnomalyDetectorCountWithTenantHeader() throws Exception {
        String tenantId = tenantId("count");
        String otherTenantId = tenantId("count-other");

        createTenantDetector(createDetectorDefinition(indexName("count")), tenantId);
        createTenantDetector(createDetectorDefinition(indexName("count-other")), otherTenantId);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response countResponse = getTenantDetectorCount(tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(countResponse));
            assertEquals(1L, detectorCount(countResponse));
        });

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response countResponse = getTenantDetectorCount(otherTenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(countResponse));
            assertEquals(1L, detectorCount(countResponse));
        });
    }

    public void testSearchAnomalyDetectorMatchWithTenantHeader() throws Exception {
        String detectorName = detectorName("match");
        String tenantId = tenantId("match");
        String otherTenantId = tenantId("match-other");

        createTenantDetector(createDetectorDefinition(indexName("match"), detectorName), tenantId);
        awaitTenantDetectorNameMatch(detectorName, tenantId, true);
        awaitTenantDetectorNameMatch(detectorName, otherTenantId, false);
    }

    public void testRunDetectorWithNoEnabledFeatureAndTenantHeader() throws Exception {
        String tenantId = tenantId("disabled-features");
        String indexName = indexName("disabled-features");
        createModelIndex(indexName, true);

        Feature disabledFeature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", false);
        AnomalyDetector detector = createTenantDetector(
            detectorBuilder(indexName, detectorName("disabled-features"), ImmutableList.of(disabledFeature)).build(),
            tenantId
        );

        ResponseException exception = expectThrows(ResponseException.class, () -> startTenantDetector(detector.getId(), tenantId));
        assertThat(exception.getMessage(), containsString("Can't start job as no enabled features configured"));
    }

    public void testBackwardCompatibilityWithOpenDistroAndTenantHeader() throws Exception {
        String tenantId = tenantId("legacy");
        AnomalyDetector detector = createDetectorDefinition(indexName("legacy"));

        Response createResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                tenantHeaders(tenantId)
            );
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(createResponse));

        @SuppressWarnings("unchecked")
        Map<String, Object> responseMap = entityAsMap(createResponse);
        String id = (String) responseMap.get("_id");
        trackTenantDetector(id, tenantId);
        assertNotEquals(AnomalyDetector.NO_ID, id);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            AnomalyDetector createdDetector = getTenantDetector(id, tenantId);
            assertEquals(id, createdDetector.getId());
        });

        Response deleteResponse = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + id,
                ImmutableMap.of(),
                "",
                tenantHeaders(tenantId)
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }

    public void testDeleteNotExistingAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("delete-missing");
        String detectorId = randomAlphaOfLength(8);

        Response response = deleteTenantDetector(detectorId, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals(detectorId, responseMap.get("_id"));
        assertEquals("deleted", responseMap.get("result"));
    }

    public void testDeleteAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("delete");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("delete")), tenantId);

        Response response = deleteTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals(detector.getId(), responseMap.get("_id"));
        assertEquals("deleted", responseMap.get("result"));

        expectThrows(ResponseException.class, () -> getTenantDetector(detector.getId(), tenantId));
    }

    public void testDeleteAnomalyDetectorWithRunningAdJobAndTenantHeader() throws Exception {
        String tenantId = tenantId("delrun");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("delrun")), tenantId);

        Response startResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));
        awaitTenantJobEnabled(detector.getId(), tenantId);

        ResponseException exception = expectThrows(ResponseException.class, () -> deleteTenantDetector(detector.getId(), tenantId));
        assertThat(exception.getMessage(), containsString("Job is running"));
    }

    public void testDeleteAnomalyDetectorWhenADDisabled() throws Exception {
        String tenantId = tenantId("delete-disabled");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("delete-disabled")), tenantId);

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> deleteTenantDetector(detector.getId(), tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }
    }

    public void testStartAdJobWithExistingDetectorAndTenantHeader() throws Exception {
        String tenantId = tenantId("start");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("start")), tenantId);

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> startTenantDetector(detector.getId(), tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }

        Response firstStartResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(firstStartResponse));

        Response secondStartResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(secondStartResponse));
    }

    public void testStartAdJobWithNonexistingDetectorAndTenantHeader() throws Exception {
        String tenantId = tenantId("start-missing");
        ResponseException exception = expectThrows(ResponseException.class, () -> startTenantDetector(randomAlphaOfLength(10), tenantId));
        assertThat(exception.getMessage(), containsString(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
    }

    public void testStopAdJobWithTenantHeader() throws Exception {
        String tenantId = tenantId("stop");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("stop")), tenantId);

        Response startResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> stopTenantDetector(detector.getId(), tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }

        Response firstStopResponse = stopTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(firstStopResponse));
        awaitTenantJobDisabled(detector.getId(), tenantId);

        Response secondStopResponse = stopTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(secondStopResponse));
    }

    public void testStopNonExistingAdJobWithTenantHeader() throws Exception {
        String tenantId = tenantId("stop-missing");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("stop-missing")), tenantId);

        Response startResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));

        ResponseException exception = expectThrows(ResponseException.class, () -> stopTenantDetector(randomAlphaOfLength(10), tenantId));
        assertThat(exception.getMessage(), containsString(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
    }

    public void testUpdateAnomalyDetectorWithRunningAdJobAndTenantHeader() throws Exception {
        String tenantId = tenantId("uprun");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("uprun")), tenantId);

        Response startResponse = startTenantDetector(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));
        awaitTenantJobEnabled(detector.getId(), tenantId);

        AnomalyDetector latestDetector = getTenantDetector(detector.getId(), tenantId);
        AnomalyDetector updatedDetector = copyDetector(
            latestDetector,
            latestDetector.getId(),
            latestDetector.getVersion(),
            latestDetector.getName(),
            randomAlphaOfLength(12)
        );

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> updateTenantDetector(latestDetector.getId(), updatedDetector, tenantId)
        );
        assertThat(exception.getMessage(), containsString("Job is running"));
    }

    public void testStartAdJobWithNullFeaturesAndTenantHeader() throws Exception {
        assertCannotStartFeaturelessDetector(true, "start-null-features");
    }

    public void testStartAdJobWithEmptyFeaturesAndTenantHeader() throws Exception {
        assertCannotStartFeaturelessDetector(false, "start-empty-features");
    }

    public void testDefaultProfileAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("profile-default");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("profile-default")), tenantId);

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> getTenantDetectorProfile(detector.getId(), tenantId));
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }

        Response profileResponse = getTenantDetectorProfile(detector.getId(), tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testAllProfileAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("profile-all");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("profile-all")), tenantId);

        Response profileResponse = getTenantDetectorProfile(detector.getId(), true, "", tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testCustomizedProfileAnomalyDetectorWithTenantHeader() throws Exception {
        String tenantId = tenantId("profile-models");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("profile-models")), tenantId);

        Response profileResponse = getTenantDetectorProfile(detector.getId(), true, "/models/", tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testUpdateAnomalyDetectorWhenADDisabled() throws Exception {
        String tenantId = tenantId("update-disabled");
        AnomalyDetector detector = createTenantDetector(createDetectorDefinition(indexName("update-disabled")), tenantId);

        AnomalyDetector updatedDetector = copyDetector(
            detector,
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            randomAlphaOfLength(12)
        );

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        try {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> updateTenantDetector(detector.getId(), updatedDetector, tenantId)
            );
            assertThat(exception.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));
        } finally {
            updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> detectorValidationMessages(Response response) throws IOException {
        return (Map<String, Map<String, String>>) XContentMapValues.extractValue("detector", entityAsMap(response));
    }

    private AnomalyDetector createHCDetectorDefinition(String indexName) throws IOException {
        String categoryField = "keyword-field";
        createModelHCADIndex(
            indexName,
            ImmutableMap.of(categoryField, "keyword"),
            String.format(Locale.ROOT, "{\"%s\":\"field-1\",\"%s\":1,\"%s\":42}", categoryField, TIME_FIELD, VALUE_FIELD)
        );
        return detectorBuilder(indexName, detectorName("validate-hc-limit")).setCategoryFields(ImmutableList.of(categoryField)).build();
    }

    private void assertMaxHCDetectorValidationIssue(AnomalyDetector detector, String tenantId, String expectedErrorMessage)
        throws IOException {
        Response response = validateTenantDetector(detector, "detector", tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Map<String, String>> validationMessages = detectorValidationMessages(response);
        assertNotNull(validationMessages);
        assertNotNull(validationMessages.get("general_settings"));
        assertEquals(expectedErrorMessage, validationMessages.get("general_settings").get("message"));
    }

    private String detectorNameQuery(String detectorName) {
        return new SearchSourceBuilder().query(QueryBuilders.matchPhraseQuery("name", detectorName)).toString();
    }

    private String detectorIdQuery(String detectorId) {
        return new SearchSourceBuilder().query(QueryBuilders.termQuery("_id", detectorId)).toString();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> searchHits(Response response) throws IOException {
        return (List<Map<String, Object>>) ((Map<String, Object>) entityAsMap(response).get("hits")).get("hits");
    }

    private long detectorCount(Response response) throws IOException {
        return ((Number) entityAsMap(response).get("count")).longValue();
    }

    private boolean detectorNameExists(Response response) throws IOException {
        return (boolean) entityAsMap(response).get("match");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> topAnomalyBuckets(Response response) throws IOException {
        return (List<Map<String, Object>>) XContentMapValues.extractValue("buckets", entityAsMap(response));
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> bucketKey(Map<String, Object> bucket) {
        return (Map<String, String>) bucket.get("key");
    }

    private void awaitTenantJobEnabled(String detectorId, String tenantId) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            ToXContentObject[] configAndJob = getTenantDetector(detectorId, true, tenantId);
            assertNotNull(configAndJob[1]);
            assertTrue(((Job) configAndJob[1]).isEnabled());
        });
    }

    private void awaitTenantJobDisabled(String detectorId, String tenantId) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            ToXContentObject[] configAndJob = getTenantDetector(detectorId, true, tenantId);
            assertNotNull(configAndJob[1]);
            assertFalse(((Job) configAndJob[1]).isEnabled());
            assertNotNull(((Job) configAndJob[1]).getDisabledTime());
        });
    }

    private void awaitTenantDetectorNameMatch(String detectorName, String tenantId, boolean expected) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response matchResponse = getTenantDetectorMatch(detectorName, tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(matchResponse));
            assertEquals(expected, detectorNameExists(matchResponse));
        });
    }

    private String duplicateDetectorMessage(String detectorName, String detectorId) {
        return String.format(Locale.ROOT, DUPLICATE_DETECTOR_MSG, detectorName, "[" + detectorId + "]");
    }

    private boolean isAossDataPlane() {
        return "aoss".equalsIgnoreCase(System.getProperty("tests.opensearch.plugins.anomaly_detection.remote_metadata_service_name", ""));
    }

    private void assertCannotStartFeaturelessDetector(boolean useNullFeatures, String suffix) throws Exception {
        String tenantId = tenantId(suffix);
        AnomalyDetector detector = createTenantDetector(
            createFeaturelessDetectorDefinition(indexName(suffix), detectorName(suffix), useNullFeatures),
            tenantId
        );

        ResponseException exception = expectThrows(ResponseException.class, () -> startTenantDetector(detector.getId(), tenantId));
        assertThat(exception.getMessage(), containsString("Can't start job as no features configured"));
    }

}
