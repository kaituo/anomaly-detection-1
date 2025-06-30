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

package org.opensearch.timeseries.rest.handler;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.transport.TransportService;

/**
 * Get job to make sure job has been stopped before updating a config.
 */
public class ConfigUpdateConfirmer<IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>> {

    private final Logger logger = LogManager.getLogger(ConfigUpdateConfirmer.class);

    private final TaskManagerType taskManager;
    private final TransportService transportService;
    private final DataManagementType dataManagement;

    public ConfigUpdateConfirmer(TaskManagerType taskManager, TransportService transportService, DataManagementType dataManagement) {
        this.taskManager = taskManager;
        this.transportService = transportService;
        this.dataManagement = dataManagement;
    }

    /**
     * Get job for update/delete config.
     * If job exist, will return error message; otherwise, execute function.
     *
     * @param id job identifier
     * @param tenantId tenant id for multi-tenant operations
     * @param listener Listener to send response
     * @param function time series function
     * @param xContentRegistry Registry which is used for XContentParser
     */
    public void confirmJobRunning(
        String id,
        String tenantId,
        ActionListener listener,
        ExecutorFunction function,
        NamedXContentRegistry xContentRegistry
    ) {
        // forecasting and ad share the same job index
        if (dataManagement.doesJobIndexExist()) {
            taskManager.getStateManager().getJob(id, tenantId, false, ActionListener.wrap(jobOptional -> {
                if (jobOptional.isPresent()) {
                    Job adJob = jobOptional.get();
                    if (adJob.isEnabled()) {
                        listener.onFailure(new OpenSearchStatusException("Job is running: " + id, RestStatus.BAD_REQUEST));
                        return;
                    }
                }
                function.execute();
            }, exception -> {
                logger.error("Fail to get job: " + id, exception);
                listener.onFailure(exception);
            }));
        } else {
            function.execute();
        }
    }

    /**
     * Confirm if any historical or run once is running. If there is still any left over tasks running,
     * listener returns failure complaining task running. Otherwise, listener response returns null
     * (indicating no batch running).
     * @param configId Config id
     * @param tasks tasks to check.
     * @param listener to return response or failure.
     */
    public void confirmBatchRunning(String configId, String tenantId, List<TaskTypeEnum> tasks, ActionListener<Void> listener) {
        taskManager.getAndExecuteOnLatestConfigLevelTask(configId, tenantId, tasks, (task) -> {
            if (task.isPresent() && !task.get().isDone()) {
                // can't update config if there is task running
                String batchTaskName = task.get() instanceof ADTask ? "Historical" : "Run once";
                listener.onFailure(new OpenSearchStatusException(batchTaskName + " is running", RestStatus.BAD_REQUEST));
            } else {
                listener.onResponse(null);
            }
        }, transportService, false, listener); // false means don't reset task state as inactive/stopped state
    }
}
