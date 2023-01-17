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

import static org.opensearch.ad.constant.ADCommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.model.AnomalyDetector.MAX_RESULT_INDEX_NAME_SIZE;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorTests extends AbstractADTest {

    public void testParseAnomalyDetector() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithCustomIndex() throws IOException {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        Config detector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5),
                randomIntBetween(1, 5),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(5)),
                resultIndex
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing result index doesn't work", resultIndex, parsedDetector.getCustomResultIndex());
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testAnomalyDetectorWithInvalidCustomIndex() throws Exception {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test@@";
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> (TestHelpers
                    .randomDetector(
                        ImmutableList.of(TestHelpers.randomFeature()),
                        randomAlphaOfLength(5),
                        randomIntBetween(1, 5),
                        randomAlphaOfLength(5),
                        ImmutableList.of(randomAlphaOfLength(5)),
                        resultIndex
                    ))
            );
    }

    public void testParseAnomalyDetectorWithoutParams() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder()));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithCustomDetectionDelay() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder()));
        LOG.info(detectorString);
        TimeValue detectionInterval = new TimeValue(1, TimeUnit.MINUTES);
        TimeValue detectionWindowDelay = new TimeValue(10, TimeUnit.MINUTES);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config
            .parse(TestHelpers.parser(detectorString), detector.getId(), detector.getVersion(), detectionInterval, detectionWindowDelay);
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseSingleEntityAnomalyDetector() throws IOException {
        Config detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomUiMetadata(),
                Instant.now(),
                AnomalyDetectorType.SINGLE_ENTITY.name()
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseHistoricalAnomalyDetectorWithoutUser() throws IOException {
        Config detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomUiMetadata(),
                Instant.now(),
                false,
                null
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithNullFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithEmptyFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":{},"
            + "\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":"
            + "{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithWrongFilterQuery() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":"
            + "{\"aa\":\"bb\"},\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},"
            + "\"window_delay\":{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":"
            + "-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\","
            + "\"feature_enabled\":false,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithoutOptionalParams() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString), "id", 1L, null, null);
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
        assertEquals((long) parsedDetector.getShingleSize(), (long) TimeSeriesSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testParseAnomalyDetectorWithInvalidShingleSize() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"shingle_size\":-1,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithNegativeWindowDelay() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":-973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithNegativeDetectionInterval() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":-425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithIncorrectFeatureQuery() throws Exception {
        String detectorString = "{\"name\":\"todagdpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":\"bb\"}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithInvalidDetectorIntervalUnits() {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Millis\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> Config.parse(TestHelpers.parser(detectorString))
        );
        assertEquals(
            String.format(Locale.ROOT, ADCommonMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
            exception.getMessage()
        );
    }

    public void testParseAnomalyDetectorInvalidWindowDelayUnits() {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Millis\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> Config.parse(TestHelpers.parser(detectorString))
        );
        assertEquals(
            String.format(Locale.ROOT, ADCommonMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
            exception.getMessage()
        );
    }

    public void testParseAnomalyDetectorWithNullUiMetadata() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
        assertNull(parsedDetector.getUiMetadata());
    }

    public void testParseAnomalyDetectorWithEmptyUiMetadata() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testInvalidShingleSize() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    0,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testNullDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    null,
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testBlankDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    "",
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testNullTimeField() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testNullIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testEmptyIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testNullDetectionInterval() throws Exception {
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    null,
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testInvalidDetectionInterval() {
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
                TestHelpers.randomIntervalTimeConfiguration(),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null
            )
        );
        assertEquals("Detection interval must be a positive integer", exception.getMessage());
    }

    public void testInvalidWindowDelay() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Config(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(1, ChronoUnit.MINUTES),
                new IntervalTimeConfiguration(-1, ChronoUnit.MINUTES),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null
            )
        );
        assertEquals("Interval -1 should be non-negative", exception.getMessage());
    }

    public void testNullFeatures() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(null, null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testEmptyFeatures() throws IOException {
        Config detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(), null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testGetShingleSize() throws IOException {
        Config anomalyDetector = new Config(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            5,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), 5);
    }

    public void testGetShingleSizeReturnsDefaultValue() throws IOException {
        Config anomalyDetector = new Config(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), TimeSeriesSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testNullFeatureAttributes() throws IOException {
        Config anomalyDetector = new Config(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            null,
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertNotNull(anomalyDetector.getFeatureAttributes());
        assertEquals(0, anomalyDetector.getFeatureAttributes().size());
    }

    public void testValidateResultIndex() {
        String errorMessage = Config.validateResultIndex("abc");
        assertEquals(ADCommonMessages.INVALID_RESULT_INDEX_PREFIX, errorMessage);

        StringBuilder resultIndexNameBuilder = new StringBuilder(CUSTOM_RESULT_INDEX_PREFIX);
        for (int i = 0; i < MAX_RESULT_INDEX_NAME_SIZE - CUSTOM_RESULT_INDEX_PREFIX.length(); i++) {
            resultIndexNameBuilder.append("a");
        }
        assertNull(Config.validateResultIndex(resultIndexNameBuilder.toString()));
        resultIndexNameBuilder.append("a");

        errorMessage = Config.validateResultIndex(resultIndexNameBuilder.toString());
        assertEquals(Config.INVALID_RESULT_INDEX_NAME_SIZE, errorMessage);

        errorMessage = Config.validateResultIndex(CUSTOM_RESULT_INDEX_PREFIX + "abc#");
        assertEquals(CommonMessages.INVALID_CHAR_IN_RESULT_INDEX_NAME, errorMessage);
    }

    public void testParseAnomalyDetectorWithNoDescription() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        Config parsedDetector = Config.parse(TestHelpers.parser(detectorString), "id", 1L, null, null);
        assertEquals(parsedDetector.getDescription(), "");
    }
}
