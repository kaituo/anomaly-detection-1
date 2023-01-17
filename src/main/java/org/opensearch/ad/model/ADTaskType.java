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

import java.util.List;

import org.opensearch.timeseries.model.TaskType;

import com.google.common.collect.ImmutableList;

public enum ADTaskType implements TaskType {
    @Deprecated
    HISTORICAL,
    AD_REALTIME_SINGLE_STREAM,
    AD_REALTIME_HC_DETECTOR,
    AD_HISTORICAL_SINGLE_STREAM,
    // detector level task to track overall state, init progress, error etc. for HC detector
    AD_HISTORICAL_HC_DETECTOR,
    // entity level task to track just one specific entity's state, init progress, error etc.
    AD_HISTORICAL_HC_ENTITY;

    public static List<ADTaskType> HISTORICAL_DETECTOR_TASK_TYPES = ImmutableList
        .of(ADTaskType.AD_HISTORICAL_HC_DETECTOR, ADTaskType.AD_REALTIME_SINGLE_STREAM, ADTaskType.HISTORICAL);
    public static List<ADTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
        .of(
            ADTaskType.AD_HISTORICAL_HC_DETECTOR,
            ADTaskType.AD_REALTIME_SINGLE_STREAM,
            ADTaskType.AD_HISTORICAL_HC_ENTITY,
            ADTaskType.HISTORICAL
        );
    public static List<ADTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ADTaskType.AD_REALTIME_SINGLE_STREAM, ADTaskType.AD_REALTIME_HC_DETECTOR);
    public static List<ADTaskType> ALL_DETECTOR_TASK_TYPES = ImmutableList
        .of(
            ADTaskType.AD_REALTIME_SINGLE_STREAM,
            ADTaskType.AD_REALTIME_HC_DETECTOR,
            ADTaskType.AD_HISTORICAL_SINGLE_STREAM,
            ADTaskType.AD_HISTORICAL_HC_DETECTOR,
            ADTaskType.HISTORICAL
        );
}
