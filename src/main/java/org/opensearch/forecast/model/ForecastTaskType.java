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

import org.opensearch.timeseries.model.TaskType;

import com.google.common.collect.ImmutableList;

public enum ForecastTaskType implements TaskType {
    FORECAST_REALTIME_SINGLE_STREAM,
    FORECAST_REALTIME_HC_FORECASTER,
    FORECAST_RUN_ONCE_SINGLE_STREAM,
    // forecaster level task to track overall state, init progress, error etc. for HC forecaster
    FORECAST_RUN_ONCE_HC_FORECASTER,
    // entity level task to track just one specific entity's state, init progress, error etc.
    FORECAST_RUN_ONCE_HC_ENTITY;

    public static List<ForecastTaskType> RUN_ONCE_FORECASTER_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.FORECAST_RUN_ONCE_HC_FORECASTER, ForecastTaskType.FORECAST_RUN_ONCE_SINGLE_STREAM);
    public static List<ForecastTaskType> ALL_RUN_ONCE_TASK_TYPES = ImmutableList
        .of(
            ForecastTaskType.FORECAST_RUN_ONCE_HC_FORECASTER,
            ForecastTaskType.FORECAST_RUN_ONCE_SINGLE_STREAM,
            ForecastTaskType.FORECAST_RUN_ONCE_HC_ENTITY
        );
    public static List<ForecastTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM, ForecastTaskType.FORECAST_REALTIME_HC);
    public static List<ForecastTaskType> ALL_FORECAST_TASK_TYPES = ImmutableList
        .of(
            ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM,
            ForecastTaskType.FORECAST_REALTIME_HC,
            ForecastTaskType.FORECAST_RUN_ONCE_SINGLE_STREAM,
            ForecastTaskType.FORECAST_RUN_ONCE_HC_FORECASTER,
            ForecastTaskType.FORECAST_RUN_ONCE_HC_ENTITY
        );
}
