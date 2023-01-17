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

package org.opensearch.forecast.model;

import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.ad.model.ADTaskType;

import com.google.common.collect.ImmutableList;

public enum ForecastTaskType {
    REALTIME_SINGLE_STREAM,
    REALTIME_HC_DETECTOR,
    HISTORICAL_SINGLE_STREAM,
    // forecaster level task to track overall state, init progress, error etc. for HC forecaster
    HISTORICAL_HC_FORECASTER,
    // entity level task to track just one specific entity's state, init progress, error etc.
    HISTORICAL_HC_ENTITY;

    public static List<ForecastTaskType> HISTORICAL_FORECASTER_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.HISTORICAL_HC_FORECASTER, ForecastTaskType.HISTORICAL_SINGLE_STREAM);
    public static List<ADTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
            .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY, ADTaskType.HISTORICAL_HC_ENTITY);
    public static List<ForecastTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.REALTIME_SINGLE_STREAM, ForecastTaskType.REALTIME_HC_DETECTOR);
    public static List<ForecastTaskType> ALL_DETECTOR_TASK_TYPES = ImmutableList
        .of(
                ForecastTaskType.REALTIME_SINGLE_STREAM,
                ForecastTaskType.REALTIME_HC_DETECTOR,
                ForecastTaskType.HISTORICAL_SINGLE_STREAM,
                ForecastTaskType.HISTORICAL_HC_FORECASTER,
                ForecastTaskType.HISTORICAL_HC_ENTITY
        );

    public static List<String> taskTypeToString(List<ForecastTaskType> forecastTaskTypes) {
        return forecastTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }
}
