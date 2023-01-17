package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker.Origin;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointWriteWorker;
import org.opensearch.timeseries.util.DateUtils;

public abstract class TimeSeriesHCCacheBuffer<RCFModelType, EntityModelState extends TimeSeriesModelState<EntityModel<RCFModelType>>, NodeState extends ExpiringState,
CheckpointDaoType extends TimeSeriesCheckpointDao<RCFModelType>,
CheckpointWriterType extends TimeSeriesCheckpointWriteWorker<NodeState, RCFModelType, CheckpointDaoType>,
CheckpointMaintainerType extends TimeSeriesCheckpointMaintainWorker<NodeState>> extends
TimeSeriesCacheBuffer<RCFModelType, NodeState, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType> {

    private static final Logger LOG = LogManager.getLogger(TimeSeriesHCCacheBuffer.class);

    protected final PriorityTracker priorityTracker;
    // key is model id
    protected final ConcurrentHashMap<String, EntityModelState> items;

    public TimeSeriesHCCacheBuffer(int minimumCapacity, Clock clock, TimeSeriesMemoryTracker memoryTracker,
            int checkpointIntervalHrs, Duration modelTtl, long memoryConsumptionPerEntity,
            CheckpointWriterType checkpointWriteQueue, CheckpointMaintainerType checkpointMaintainQueue,
            String configId, long intervalSecs, Origin origin) {
        super(minimumCapacity, clock, memoryTracker, checkpointIntervalHrs, modelTtl, memoryConsumptionPerEntity,
                checkpointWriteQueue, checkpointMaintainQueue, configId, origin);
        this.priorityTracker = new PriorityTracker(clock, intervalSecs, clock.instant().getEpochSecond(), MAX_TRACKING_ENTITIES);
        this.items = new ConcurrentHashMap<>();
    }

    /**
     * Remove everything associated with the key and make a checkpoint if input specified so.
     *
     * @param keyToRemove The key to remove
     * @param saveCheckpoint Whether saving checkpoint or not
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public EntityModelState remove(String keyToRemove, boolean saveCheckpoint) {
        priorityTracker.removePriority(keyToRemove);

        // if shared cache is empty, we are using reserved memory
        boolean reserved = sharedCacheEmpty();

        EntityModelState valueRemoved = items.remove(keyToRemove);

        if (valueRemoved != null) {
            if (!reserved) {
                // release in shared memory
                memoryTracker.releaseMemory(memoryConsumptionPerModel, false, Origin.HC_DETECTOR);
            }

            EntityModel<RCFModelType> modelRemoved = valueRemoved.getModel();
            if (modelRemoved != null) {
                if (saveCheckpoint) {
                    // null model has only samples. For null model we save a checkpoint
                    // regardless of last checkpoint time. whether If we don't save,
                    // we throw the new samples and might never be able to initialize the model
                    boolean isNullModel = !modelRemoved.getModel().isPresent();
                    checkpointWriteQueue.write(valueRemoved, isNullModel, RequestPriority.MEDIUM);
                }

                modelRemoved.clear();
            }
        }

        return valueRemoved;
    }

    /**
     * Remove everything associated with the key and make a checkpoint.
     *
     * @param keyToRemove The key to remove
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public EntityModelState remove(String keyToRemove) {
        return remove(keyToRemove, true);
    }

    public PriorityTracker getPriorityTracker() {
        return priorityTracker;
    }

    /**
     * remove the smallest priority item.
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public EntityModelState remove() {
        // race conditions can happen between the put and one of the following operations:
        // remove from other threads: not a problem. If they remove the same item,
        // our method is idempotent. If they remove two different items,
        // they don't impact each other.
        // maintenance: not a problem as all of the data structures are concurrent.
        // Two threads removing the same entry is not a problem.
        // clear: not a problem as we are releasing memory in MemoryTracker.
        // The removed one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        // put: not a problem as it is unlikely we are removing and putting the same thing
        Optional<String> key = priorityTracker.getMinimumPriorityEntityId();
        if (key.isPresent()) {
            return remove(key.get());
        }
        return null;
    }

    /**
     * Remove expired state and save checkpoints of existing states
     * @return removed states
     */
    public List<EntityModelState> maintenance() {
        List<CheckpointMaintainRequest> modelsToSave = new ArrayList<>();
        List<EntityModelState> removedStates = new ArrayList<>();
        Instant now = clock.instant();
        int currentHour = DateUtils.getUTCHourOfDay(now);
        int currentSlot = currentHour % checkpointIntervalHrs;
        items.entrySet().stream().forEach(entry -> {
            String entityModelId = entry.getKey();
            try {
                EntityModelState modelState = entry.getValue();

                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    // race conditions can happen between the put and one of the following operations:
                    // remove: not a problem as all of the data structures are concurrent.
                    // Two threads removing the same entry is not a problem.
                    // clear: not a problem as we are releasing memory in MemoryTracker.
                    // The removed one loses references and soon GC will collect it.
                    // We have memory tracking correction to fix incorrect memory usage record.
                    // put: not a problem as we are unlikely to maintain an entry that's not
                    // already in the cache
                    // remove method saves checkpoint as well
                    removedStates.add(remove(entityModelId));
                } else if (Math.abs(entityModelId.hashCode()) % checkpointIntervalHrs == currentSlot) {
                    // checkpoint is relatively big compared to other queued requests
                    // Evens out the resource usage more fairly across a large maintenance window
                    // by adding saving requests to CheckpointMaintainWorker.
                    //
                    // Background:
                    // We will save a checkpoint when
                    //
                    // (a)removing the model from cache.
                    // (b) cold start
                    // (c) no complete model only a few samples. If we don't save new samples,
                    // we will never be able to have enough samples for a trained mode.
                    // (d) periodically save in case of exceptions.
                    //
                    // This branch is doing d). Previously, I will do it every hour for all
                    // in-cache models. Consider we are moving to 1M entities, this will bring
                    // the cluster in a heavy payload every hour. That's why I am doing it randomly
                    // (expected 6 hours for each checkpoint statistically).
                    //
                    // I am doing it random since maintaining a state of which one has been saved
                    // and which one hasn't are not cheap. Also, the models in the cache can be
                    // dynamically changing. Will have to maintain the state in the removing logic.
                    // Random is a lazy way to deal with this as it is stateless and statistically sound.
                    //
                    // If a checkpoint does not fall into the 6-hour bucket in a particular scenario, the model
                    // is stale (i.e., we don't recover from the freshest model in disaster.).
                    //
                    // All in all, randomness is mostly due to performance and easy maintenance.
                    modelsToSave
                        .add(
                            new CheckpointMaintainRequest(
                                // the request expires when the next maintainance starts
                                System.currentTimeMillis() + modelTtl.toMillis(),
                                configId,
                                RequestPriority.LOW,
                                entityModelId
                            )
                        );
                }

            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for model id " + entityModelId, e);
            }
        });

        checkpointMaintainQueue.putAll(modelsToSave);
        return removedStates;
    }

    /**
    *
    * @return whether there is one item that can be removed from shared cache
    */
   public boolean canRemove() {
       return !items.isEmpty() && items.size() > minimumCapacity;
   }

   /**
    * @return whether shared cache is empty or not
    */
   public boolean sharedCacheEmpty() {
       return items.size() <= minimumCapacity;
   }

   /**
    * @return whether dedicated cache is available or not
    */
   public boolean dedicatedCacheAvailable() {
       return items.size() < minimumCapacity;
   }

   /**
   *
   * @return the number of active entities
   */
  public int getActiveEntities() {
      return items.size();
  }

  /**
   *
   * @param entityModelId Model Id
   * @return Whether the model is active or not
   */
  public boolean isActive(String entityModelId) {
      return items.containsKey(entityModelId);
  }

  /**
   *
   * @param entityModelId Model Id
   * @return Last used time of the model
   */
  public long getLastUsedTime(String entityModelId) {
      EntityModelState state = items.get(entityModelId);
      if (state != null) {
          return state.getLastUsedTime().toEpochMilli();
      }
      return -1;
  }

  /**
   *
   * @param entityModelId entity Id
   * @return Get the model of an entity
   */
  public Optional<EntityModel<RCFModelType>> getModel(String entityModelId) {
      return Optional.of(items).map(map -> map.get(entityModelId)).map(state -> state.getModel());
  }

  /**
  *
  * @return bytes consumed in the shared cache by the CacheBuffer
  */
 public long getBytesInSharedCache() {
     int sharedCacheEntries = items.size() - minimumCapacity;
     if (sharedCacheEntries > 0) {
         return memoryConsumptionPerModel * sharedCacheEntries;
     }
     return 0;
 }

 /**
  * Update step at period t_k:
  * new priority = old priority + log(1+e^{\log(g(t_k-L))-old priority}) where g(n) = e^{0.125n},
  * and n is the period.
  * @param entityModelId model Id
  */
 private void update(String entityModelId) {
     priorityTracker.updatePriority(entityModelId);

     Instant now = clock.instant();
     items.get(entityModelId).setLastUsedTime(now);
     lastUsedTime = now;
 }

 /**
  * Insert the model state associated with a model Id to the cache
  * @param entityModelId the model Id
  * @param value the ModelState
  */
 public void put(String entityModelId, EntityModelState value) {
     // race conditions can happen between the put and one of the following operations:
     // remove: not a problem as it is unlikely we are removing and putting the same thing
     // maintenance: not a problem as we are unlikely to maintain an entry that's not
     // already in the cache
     // clear: not a problem as we are releasing memory in MemoryTracker.
     // The newly added one loses references and soon GC will collect it.
     // We have memory tracking correction to fix incorrect memory usage record.
     // put from other threads: not a problem as the entry is associated with
     // entityModelId and our put is idempotent
     put(entityModelId, value, value.getPriority());
 }

 /**
 * Insert the model state associated with a model Id to the cache.  Update priority.
 * @param entityModelId the model Id
 * @param value the ModelState
 * @param priority the priority
 */
 private void put(String entityModelId, EntityModelState value, float priority) {
     EntityModelState contentNode = items.get(entityModelId);
     if (contentNode == null) {
         priorityTracker.addPriority(entityModelId, priority);
         items.put(entityModelId, value);
         Instant now = clock.instant();
         value.setLastUsedTime(now);
         lastUsedTime = now;
         // shared cache empty means we are consuming reserved cache.
         // Since we have already considered them while allocating CacheBuffer,
         // skip bookkeeping.
         if (!sharedCacheEmpty()) {
             memoryTracker.consumeMemory(memoryConsumptionPerModel, false, Origin.HC_DETECTOR);
         }
     } else {
         update(entityModelId);
         items.put(entityModelId, value);
     }
 }

 /**
  * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
  * contains no mapping for the model Id
  * @param key the model Id
  * @return the Model state to which the specified model Id is mapped, or null
  * if this CacheBuffer contains no mapping for the model Id
  */
 public EntityModelState get(String key) {
     // We can get an item that is to be removed soon due to race condition.
     // This is acceptable as it won't cause any corruption and exception.
     // And this item is used for scoring one last time.
     EntityModelState node = items.get(key);
     if (node == null) {
         return null;
     }
     update(key);
     return node;
 }

 /**
  * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
  * contains no mapping for the model Id. Compared to get method, the method won't
  * increment entity priority. Used in cache buffer maintenance.
  *
  * @param key the model Id
  * @return the Model state to which the specified model Id is mapped, or null
  * if this CacheBuffer contains no mapping for the model Id
  */
 public EntityModelState getWithoutUpdatePriority(String key) {
     // We can get an item that is to be removed soon due to race condition.
     // This is acceptable as it won't cause any corruption and exception.
     // And this item is used for scoring one last time.
     EntityModelState node = items.get(key);
     if (node == null) {
         return null;
     }
     return node;
 }

 /**
  *
  * If the cache is not full, check if some other items can replace internal entities
  * within the same detector.
  *
  * @param priority another entity's priority
  * @return whether one entity can be replaced by another entity with a certain priority
  */
 public boolean canReplaceWithinDetector(float priority) {
     if (items.isEmpty()) {
         return false;
     }
     Optional<Entry<String, Float>> minPriorityItem = priorityTracker.getMinimumPriority();
     return minPriorityItem.isPresent() && priority > minPriorityItem.get().getValue();
 }

 /**
  * Replace the smallest priority entity with the input entity
  * @param entityModelId the Model Id
  * @param value the model State
  * @return the associated ModelState associated with the key, or null if there
  * is no associated ModelState for the key
  */
 public EntityModelState replace(String entityModelId, EntityModelState value) {
     EntityModelState replaced = remove();
     put(entityModelId, value);
     return replaced;
 }

 public List<EntityModelState> getAllModels() {
     return items.values().stream().collect(Collectors.toList());
 }

 /**
  * Clear associated memory.  Used when we are removing an detector.
  */
 @Override
public void clear() {
     // race conditions can happen between the put and remove/maintenance/put:
     // not a problem as we are releasing memory in MemoryTracker.
     // The newly added one loses references and soon GC will collect it.
     // We have memory tracking correction to fix incorrect memory usage record.
     memoryTracker.releaseMemory(getReservedBytes(), true, origin);
     if (!sharedCacheEmpty()) {
         memoryTracker.releaseMemory(getBytesInSharedCache(), false, origin);
     }
     items.clear();
     priorityTracker.clearPriority();
 }
}
