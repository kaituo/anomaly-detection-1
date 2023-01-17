package org.opensearch.forecast.task;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.task.RealtimeTaskCache;

public class ForecastTaskCacheManager {
    private final Logger logger = LogManager.getLogger(ForecastTaskCacheManager.class);

    /**
     * This field is to cache all realtime tasks on coordinating node.
     * <p>Node: coordinating node</p>
     * <p>Key is detector id</p>
     */
    private Map<String, RealtimeTaskCache> realtimeTaskCaches;

    /**
     * This field is to cache all deleted detector level tasks on coordinating node.
     * Will try to clean up child task and AD result later.
     * <p>Node: coordinating node</p>
     * Check {@link ForecastTaskManager#cleanChildTasksAndResultsOfDeletedTask()}
     */
    private Queue<String> deletedForecasterTasks;

    private volatile Integer maxCachedDeletedTask;

    public ForecastTaskCacheManager(Settings settings, ClusterService clusterService) {
        this.realtimeTaskCaches = new ConcurrentHashMap<>();
        this.deletedForecasterTasks = new ConcurrentLinkedQueue<>();
        this.maxCachedDeletedTask = MAX_CACHED_DELETED_TASKS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CACHED_DELETED_TASKS, it -> maxCachedDeletedTask = it);
    }

    public RealtimeTaskCache getRealtimeTaskCache(String detectorId) {
        return realtimeTaskCaches.get(detectorId);
    }

    public void initRealtimeTaskCache(String forecasterId, long forecasterIntervalInMillis) {
        realtimeTaskCaches.put(forecasterId, new RealtimeTaskCache(null, null, null, forecasterIntervalInMillis));
        logger.debug("Realtime task cache inited");
    }

    /**
     * Add deleted task's id to deleted detector tasks queue.
     * @param taskId task id
     */
    public void addDeletedDetectorTask(String taskId) {
        if (deletedForecasterTasks.size() < maxCachedDeletedTask) {
            deletedForecasterTasks.add(taskId);
        }
    }

    /**
     * Check if deleted task queue has items.
     * @return true if has deleted detector task in cache
     */
    public boolean hasDeletedDetectorTask() {
        return !deletedForecasterTasks.isEmpty();
    }

    /**
     * Poll one deleted detector task.
     * @return task id
     */
    public String pollDeletedDetectorTask() {
        return this.deletedForecasterTasks.poll();
    }

    /**
     * Add deleted detector's id to deleted detector queue.
     * @param detectorId detector id
     */
    public void addDeletedDetector(String detectorId) {
        if (deletedForecasterTasks.size() < maxCachedDeletedTask) {
            deletedForecasterTasks.add(detectorId);
        }
    }

    /**
     * Poll one deleted detector.
     * @return detector id
     */
    public String pollDeletedDetector() {
        return this.deletedForecasterTasks.poll();
    }

}
