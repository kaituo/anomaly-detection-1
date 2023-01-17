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

package org.opensearch.timeseries.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all external stats which will be returned in
 * AD stats REST API.
 */
public enum StatNames {
    // AD stats
    AD_EXECUTE_REQUEST_COUNT("ad_execute_request_count", StatType.AD),
    AD_EXECUTE_FAIL_COUNT("ad_execute_failure_count", StatType.AD),
    AD_HC_EXECUTE_REQUEST_COUNT("ad_hc_execute_request_count", StatType.AD),
    AD_HC_EXECUTE_FAIL_COUNT("ad_hc_execute_failure_count", StatType.AD),
    DETECTOR_COUNT("detector_count", StatType.AD),
    SINGLE_ENTITY_DETECTOR_COUNT("single_entity_detector_count", StatType.AD),
    MULTI_ENTITY_DETECTOR_COUNT("multi_entity_detector_count", StatType.AD),
    ANOMALY_DETECTORS_INDEX_STATUS("anomaly_detectors_index_status", StatType.AD),
    ANOMALY_RESULTS_INDEX_STATUS("anomaly_results_index_status", StatType.AD),
    MODELS_CHECKPOINT_INDEX_STATUS("models_checkpoint_index_status", StatType.AD),
    ANOMALY_DETECTION_JOB_INDEX_STATUS("anomaly_detection_job_index_status", StatType.AD),
    ANOMALY_DETECTION_STATE_STATUS("anomaly_detection_state_status", StatType.AD),
    MODEL_INFORMATION("models", StatType.AD),
    AD_EXECUTING_BATCH_TASK_COUNT("ad_executing_batch_task_count", StatType.AD),
    AD_CANCELED_BATCH_TASK_COUNT("ad_canceled_batch_task_count", StatType.AD),
    AD_TOTAL_BATCH_TASK_EXECUTION_COUNT("ad_total_batch_task_execution_count", StatType.AD),
    AD_BATCH_TASK_FAILURE_COUNT("ad_batch_task_failure_count", StatType.AD),
    MODEL_COUNT("model_count", StatType.AD),
    MODEL_CORRUTPION_COUNT("model_corruption_count", StatType.AD),
    // forecast stats
    FORECAST_EXECUTE_REQUEST_COUNT("forecast_execute_request_count", StatType.FORECAST),
    FORECAST_EXECUTE_FAIL_COUNT("forecast_execute_failure_count", StatType.FORECAST),
    FORECAST_HC_EXECUTE_REQUEST_COUNT("forecast_hc_execute_request_count", StatType.FORECAST),
    FORECAST_HC_EXECUTE_FAIL_COUNT("forecast_hc_execute_failure_count", StatType.FORECAST);

    private final String name;
    private final StatType type;

    StatNames(String name, StatType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    public StatType getType() {
        return type;
    }

    /**
     * Get set of stat names
     *
     * @return set of stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}
