/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.model.TimeSeriesTask;

/**
 * Break the cross dependency between ProfileRunner and TaskManager. Instead, both of them depend on TaskProfileRunner.
 *
 * @param <TaskClass> the time series task type
 * @param <TaskProfileType> the task profile type
 */
public interface TaskProfileRunner<TaskClass extends TimeSeriesTask, TaskProfileType extends TaskProfile<TaskClass>> {
    void getTaskProfile(TaskClass configLevelTask, ActionListener<TaskProfileType> listener);
}
