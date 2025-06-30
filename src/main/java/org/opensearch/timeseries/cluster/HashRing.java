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

package org.opensearch.timeseries.cluster;

import static org.opensearch.ad.constant.ADCommonName.AD_PLUGIN_NAME;
import static org.opensearch.ad.constant.ADCommonName.AD_PLUGIN_NAME_FOR_TEST;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_COOLDOWN_MINUTES;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.TransportUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.Sets;

public class HashRing {
    private static final Logger LOG = LogManager.getLogger(HashRing.class);
    private static final long[] DEFERRED_NODE_RETRY_DELAYS_MILLIS = new long[] { 20000L, 60000L, 300000L, 600000L };
    // In case of frequent node join/leave, hash ring has a cooldown period say 5 minute.
    // Hash ring doesn't respond to more than 1 cluster membership changes within the
    // cool-down period.
    static final String COOLDOWN_MSG = "Hash ring doesn't respond to cluster state change within the cooldown period.";
    private static final String DEFAULT_HASH_RING_MODEL_ID = "DEFAULT_HASHRING_MODEL_ID";
    static final String REMOVE_MODEL_MSG = "Remove model";

    private final int VIRTUAL_NODE_COUNT = 100;

    // Semaphore to control only 1 thread can build AD hash ring.
    private Semaphore buildHashRingSemaphore;
    // This field is to track time series plugin version of all nodes.
    // Key: node id; Value: node info
    private Map<String, TimeSeriesNodeInfo> nodeVersions;
    // This field records time series version hash ring in realtime way. Historical detection will use this hash ring.
    // Key: time series version; Value: hash ring which only contains eligible data nodes
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circles;
    // Track if hash ring inited or not. If not inited, the first clusterManager event will try to init it.
    private AtomicBoolean hashRingInited;

    // the UTC epoch milliseconds of the most recent successful update of AD circles for realtime AD.
    private long lastUpdateForRealtimeAD;
    // Cool down period before next hash ring rebuild. We need this as realtime AD needs stable hash ring.
    private volatile TimeValue coolDownPeriodForRealtimeAD;
    // This field records time series version hash ring with cooldown period. Realtime job will use this hash ring.
    // Key: time series version; Value: hash ring which only contains eligible data nodes
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circlesForRealtimeAD;

    // Record node change event. Will check if there is node change event when rebuild AD hash ring with
    // cooldown for realtime job.
    private ConcurrentLinkedQueue<Boolean> nodeChangeEvents;
    private Map<String, DeferredNodeProbeState> deferredNodeProbeStates;

    private final DiscoveryNodeSelector nodeFilter;
    private final ClusterService clusterService;
    private final ADDataMigrator dataMigrator;
    private final Clock clock;
    private final DataAccess dataAccess;
    private final ADCacheProvider cacheProvider;
    private final Settings settings;
    private final int opensearchPort;

    public HashRing(
        DiscoveryNodeSelector nodeFilter,
        Clock clock,
        Settings settings,
        DataAccess dataAccess,
        ClusterService clusterService,
        ADDataMigrator dataMigrator,
        ADCacheProvider cacheProvider
    ) {
        this.nodeFilter = nodeFilter;
        this.buildHashRingSemaphore = new Semaphore(1);
        this.clock = clock;
        this.settings = settings;
        this.opensearchPort = TimeSeriesSettings.OPENSEARCH_PORT.get(settings);
        this.coolDownPeriodForRealtimeAD = AD_COOLDOWN_MINUTES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_COOLDOWN_MINUTES, it -> coolDownPeriodForRealtimeAD = it);

        this.lastUpdateForRealtimeAD = 0;
        this.dataAccess = dataAccess;
        this.clusterService = clusterService;
        this.dataMigrator = dataMigrator;
        this.nodeVersions = new ConcurrentHashMap<>();
        this.circles = new TreeMap<>();
        this.circlesForRealtimeAD = new TreeMap<>();
        this.hashRingInited = new AtomicBoolean(false);
        this.nodeChangeEvents = new ConcurrentLinkedQueue<>();
        this.deferredNodeProbeStates = new ConcurrentHashMap<>();
        this.cacheProvider = cacheProvider;
    }

    public boolean isHashRingInited() {
        return hashRingInited.get();
    }

    /**
     * Build version based circles with discovery node delta change. Listen to clusterManager event in
     * {@link ClusterEventListener#clusterChanged(ClusterChangedEvent)}.
     * Will remove the removed nodes from cache and send request to newly added nodes to get their
     * plugin information; then add new nodes to version hash ring.
     *
     * @param delta discovery node delta change
     * @param listener action listener
     */
    public void buildCircles(DiscoveryNodes.Delta delta, ActionListener<Boolean> listener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("hash ring change is in progress. Can't build hash ring for node delta event.");
            listener.onResponse(false);
            return;
        }
        Set<String> removedNodeIds = delta.removed()
            ? delta.removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        Set<String> addedNodeIds = delta.added() ? delta.addedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()) : null;
        buildCircles(removedNodeIds, addedNodeIds, listener);
    }

    /**
     * Build version based circles by comparing with all eligible data nodes.
     * 1. Remove nodes which are not eligible now;
     * 2. Add nodes which are not in version circles.
     * @param actionListener action listener
     */
    public void buildCircles(ActionListener<Boolean> actionListener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("hash ring change is in progress. Can't rebuild hash ring.");
            actionListener.onResponse(false);
            return;
        }
        DiscoveryNode[] allNodes = getEligibleDataNodesForHashRing();
        Set<String> nodeIds = new HashSet<>();
        for (DiscoveryNode node : allNodes) {
            nodeIds.add(node.getId());
        }
        pruneDeferredNodeProbeStates(nodeIds);
        Set<String> currentNodeIds = nodeVersions.keySet();
        Set<String> removedNodeIds = Sets.difference(currentNodeIds, nodeIds);
        Set<String> addedNodeIds = Sets.difference(nodeIds, currentNodeIds);
        buildCircles(removedNodeIds, addedNodeIds, actionListener);
    }

    public void buildCirclesForRealtime() {
        /*
         * Caller-side change for deferred retries:
         *
         * The caller path is widened so deferred readiness retries can wake the rebuild flow even
         * without a fresh node-change event. In other words, realtime rebuild attempts are not only
         * entered through queued nodeChangeEvents, but also entered through when the ring is empty
         * and the local single node has both roles (mostly for testing).
         */
        if (nodeChangeEvents.isEmpty() && !hasDueDeferredNodeProbe() && !shouldBootstrapSingleLocalCoordinatorModelNode()) {
            return;
        }
        buildCircles(
            ActionListener.wrap(r -> { LOG.debug("build circles successfully"); }, e -> { LOG.error("Failed to build circles", e); })
        );
    }

    public boolean hasRealtimeHashRing() {
        return circlesForRealtimeAD.values().stream().anyMatch(circle -> circle != null && !circle.isEmpty());
    }

    /**
     * Build version hash ring.
     * 1. Delete removed nodes from version hash ring.
     * 2. Add new nodes to version hash ring
     *
     * If fail to acquire semaphore to update version hash ring, will return false to
     * action listener; otherwise will return true. The "true" response just mean we got
     * semaphore and finished rebuilding hash ring, but the hash ring may stay the same.
     * Hash ring changed or not depends on if "removedNodeIds" or "addedNodeIds" is empty.
     *
     * We use different way to build hash ring for realtime job and historical analysis
     * 1. For historical analysis,if node removed, we remove it immediately from version circles
     *    to avoid new task routes to it. If new node added, we add it immediately to version circles
     *    to make load more balanced and speed up task running.
     * 2. For realtime job, we don't record which node running detector's model partition. We just
     *    use hash ring to get owning node. If we rebuild hash ring frequently, realtime job may get
     *    different owning node and need to restore model on new owning node. If that happens a lot,
     *    it may bring heavy load to cluster. So we prefer to wait for some time before next hash ring
     *    rebuild, we call it cooldown period. The cons is we may have stale hash ring during cooldown
     *    period. Some node may already been removed from hash ring, then realtime job won't know this
     *    and still send RCF request to it. If new node added during cooldown period, realtime job won't
     *    choose it as model partition owning node, thus we may have skewed load on data nodes.
     *
     * [Important!]: When you call this function, make sure you TRY ACQUIRE buildHashRingSemaphore first.
     *               Check {@link HashRing#buildCircles(ActionListener)} and
     *               {@link HashRing#buildCircles(DiscoveryNodes.Delta, ActionListener)}
     *
     * @param removedNodeIds removed node ids
     * @param addedNodeIds added node ids
     * @param actionListener action listener
     */
    private void buildCircles(Set<String> removedNodeIds, Set<String> addedNodeIds, ActionListener<Boolean> actionListener) {
        if (buildHashRingSemaphore.availablePermits() != 0) {
            throw new TimeSeriesException("Must get update hash ring semaphore before building AD hash ring");
        }
        try {
            DiscoveryNode localNode = clusterService.localNode();
            /*
             * versionCirclesChanged means: did this buildCircles(...) pass actually change the
             * authoritative versioned hash ring, circles?
             *
             * It becomes true only when:
             * 1. an eligible node is removed from circles, or
             * 2. an eligible node is successfully admitted into a version circle after nodesInfo()
             *    returns.
             *
             * It does not mean "some node event happened". Deferred or not-yet-ready nodes can
             * create retry bookkeeping and keep rebuild flow active without setting this flag,
             * because the realtime ring should only be rebuilt on actual version-circle changes.
             */
            boolean versionCirclesChanged = false;
            if (removedNodeIds != null && removedNodeIds.size() > 0) {
                clearDeferredNodeProbeStates(removedNodeIds);
                LOG
                    .info(
                        "Node removed: {}",
                        Arrays.toString(removedNodeIds.toArray(new String[0])),
                        new Exception("Node removed stack trace")
                    );
                for (String nodeId : removedNodeIds) {
                    TimeSeriesNodeInfo nodeInfo = nodeVersions.remove(nodeId);
                    if (nodeInfo != null && nodeInfo.isEligibleDataNode()) {
                        removeNodeFromCircles(nodeId, nodeInfo.getVersion());
                        LOG.info("Remove data node from version hash ring: {}", nodeId);
                        versionCirclesChanged = true;
                    }
                }
            }
            Set<String> allAddedNodes = new HashSet<>();

            if (addedNodeIds != null) {
                allAddedNodes.addAll(addedNodeIds);
            }

            // Prepare for nodesInfo() call across deployment modes:
            // - Single-tenant: eligible nodes and nodesInfo() use real node IDs, so nodeVersions is keyed by node ID.
            // resolveLocalNodeVersionKey() returns localNode.getId(), matching previous behavior.
            // - Multi-tenant SDK mode: eligible nodes come from CloudMap as id=ip:port and SDK nodesInfo()
            // also uses ip:port identities. nodeVersions is therefore keyed by ip:port, so
            // resolveLocalNodeVersionKey() falls back to local transport address (ip:port) to avoid repeatedly
            // force-adding the local UUID key.
            // Only force-add local when the resolved key is an eligible node in current membership.
            String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
            if (shouldIncludeLocalNode(localNodeVersionKey) && !nodeVersions.containsKey(localNodeVersionKey)) {
                allAddedNodes.add(localNodeVersionKey);
            }
            if (allAddedNodes.size() == 0) {
                // rebuild version hash ring with cooldown.
                seedRealtimeRebuildEventIfNeeded(versionCirclesChanged);
                rebuildCirclesForRealtimeAD(versionCirclesChanged);
                buildHashRingSemaphore.release();
                hashRingInited.set(true);
                actionListener.onResponse(true);
                return;
            }

            LOG.info("Nodes discovered from membership: {}", Arrays.toString(allAddedNodes.toArray(new String[0])));
            Set<String> nodesToRefresh = getDueNodeIds(allAddedNodes);
            if (nodesToRefresh.isEmpty()) {
                seedRealtimeRebuildEventIfNeeded(versionCirclesChanged);
                rebuildCirclesForRealtimeAD(versionCirclesChanged);
                buildHashRingSemaphore.release();
                hashRingInited.set(true);
                actionListener.onResponse(true);
                return;
            }
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
            nodesInfoRequest.nodesIds(nodesToRefresh.toArray(new String[0]));
            nodesInfoRequest.clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
            boolean circlesChangedBeforeNodesInfo = versionCirclesChanged;

            dataAccess.nodesInfo(nodesInfoRequest, ActionListener.wrap(r -> {
                Map<String, NodeInfo> nodesMap = r.getNodesMap();
                boolean currentVersionCirclesChanged = circlesChangedBeforeNodesInfo;
                Set<String> admittedNodeIds = new HashSet<>();
                if (nodesMap != null && nodesMap.size() > 0) {
                    for (Map.Entry<String, NodeInfo> entry : nodesMap.entrySet()) {
                        NodeInfo nodeInfo = entry.getValue();
                        PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
                        DiscoveryNode curNode = nodeInfo.getNode();
                        if (plugins == null) {
                            continue;
                        }
                        TreeMap<Integer, DiscoveryNode> circle = null;
                        boolean versionRecorded = false;
                        for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                            // bwc: Need to include both.
                            if (AD_PLUGIN_NAME.equals(pluginInfo.getName())
                                || AD_PLUGIN_NAME_FOR_TEST.equals(pluginInfo.getName())
                                || CommonName.TIME_SERIES_PLUGIN_NAME.equals(pluginInfo.getName())
                                || CommonName.TIME_SERIES_PLUGIN_NAME_FOR_TEST.equals(pluginInfo.getName())) {
                                Version version = VersionUtil.fromString(pluginInfo.getVersion());
                                boolean eligibleNode = nodeFilter.isEligibleNode(curNode);
                                if (eligibleNode) {
                                    circle = circles.computeIfAbsent(version, key -> new TreeMap<>());
                                    LOG.info("Add data node to version hash ring: {}", curNode.getId());
                                }
                                nodeVersions.put(curNode.getId(), new TimeSeriesNodeInfo(version, eligibleNode));
                                versionRecorded = true;
                                break;
                            }
                        }
                        if (versionRecorded) {
                            admittedNodeIds.add(curNode.getId());
                            if (circle != null) {
                                currentVersionCirclesChanged = true;
                            }
                        }
                        if (circle != null) {
                            for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                                circle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                            }
                        }
                    }
                }
                clearDeferredNodeProbeStates(admittedNodeIds);
                Set<String> deferredNodeIds = new HashSet<>(nodesToRefresh);
                deferredNodeIds.removeAll(admittedNodeIds);
                recordDeferredNodeProbeFailures(deferredNodeIds);
                if (!admittedNodeIds.isEmpty()) {
                    LOG.info("Node version admitted: {}", Arrays.toString(admittedNodeIds.toArray(new String[0])));
                }
                if (!deferredNodeIds.isEmpty()) {
                    LOG.info("Node deferred for readiness retry: {}", Arrays.toString(deferredNodeIds.toArray(new String[0])));
                }
                LOG.info("All nodes with known version: {}", nodeVersions);

                // rebuild version hash ring with cooldown after all new node added.
                seedRealtimeRebuildEventIfNeeded(currentVersionCirclesChanged);
                rebuildCirclesForRealtimeAD(currentVersionCirclesChanged);

                if (!dataMigrator.isMigrated() && circles.size() > 0) {
                    // Find owning node with highest version to make sure the data migration logic be compatible to
                    // latest version when upgrade.
                    Optional<DiscoveryNode> owningNode = getOwningNodeWithHighestVersion(DEFAULT_HASH_RING_MODEL_ID);
                    String localNodeId = localNode.getId();
                    if (owningNode.isPresent() && localNodeId.equals(owningNode.get().getId())) {
                        dataMigrator.migrateData();
                    } else {
                        dataMigrator.skipMigration();
                    }
                }
                buildHashRingSemaphore.release();
                hashRingInited.set(true);
                actionListener.onResponse(true);
            }, e -> {
                buildHashRingSemaphore.release();
                actionListener.onFailure(e);
                LOG.error("Fail to get node info to build hash ring", e);
            }));
        } catch (Exception e) {
            LOG.error("Failed to build circles", e);
            buildHashRingSemaphore.release();
            actionListener.onFailure(e);
        }
    }

    private void removeNodeFromCircles(String nodeId, Version version) {
        if (version != null) {
            TreeMap<Integer, DiscoveryNode> circle = this.circles.get(version);
            List<Integer> deleted = new ArrayList<>();
            for (Map.Entry<Integer, DiscoveryNode> entry : circle.entrySet()) {
                if (entry.getValue().getId().equals(nodeId)) {
                    deleted.add(entry.getKey());
                }
            }
            if (deleted.size() == circle.size()) {
                circles.remove(version);
            } else {
                for (Integer key : deleted) {
                    circle.remove(key);
                }
            }
        }
    }

    /**
     * Rebuild the realtime AD hash ring only when this build pass actually changed the
     * authoritative version circles.
     *
     * Behavior:
     * 1. A full realtime rebuild happens only when versionCirclesChanged is true, meaning this
     *    buildCircles(...) pass actually changed the authoritative versioned hash ring, circles.
     * 2. If versionCirclesChanged is false and deferred node probes are pending, drain queued
     *    node-change events and wait for readiness retries instead of churning the realtime ring.
     * 3. If versionCirclesChanged is false and there are only queued node-change events, consume
     *    those events and return without copying circles into circlesForRealtimeAD.
     * 4. Only when versionCirclesChanged is true do we copy circles into circlesForRealtimeAD,
     *    update lastUpdateForRealtimeAD, and rebalance model ownership.
     * 5. If buildCircles(...) changed circles without a queued node-change event, a caller seeds
     *    exactly one synthetic event before invoking this method so cooldown-delayed realtime
     *    rebuilds can still happen once they become eligible.
     *
     * Net effect:
     * realtime AD hash ring rebuilds now happen on actual ring changes, not just on any
     * membership signal. Newly discovered but not-yet-ready nodes are retried separately, which
     * avoids unnecessary realtime ring churn.
     *
     * @param versionCirclesChanged true only when this build pass changed circles by removing an
     *                              eligible node or admitting an eligible node into a version
     *                              circle after nodesInfo() returned
     */
    private void rebuildCirclesForRealtimeAD(boolean versionCirclesChanged) {
        // Check if it's eligible to rebuild hash ring with cooldown
        if (eligibleToRebuildCirclesForRealtimeAD()) {
            int size = nodeChangeEvents.size();
            if (!versionCirclesChanged && hasPendingDeferredNodeProbe()) {
                consumeNodeChangeEvents(size);
                LOG.debug("Skip realtime hash ring rebuild while waiting for deferred node readiness retries.");
                return;
            }
            if (!versionCirclesChanged && size == 0) {
                LOG.info("No node change events, skip rebuild hash ring for realtime");
                return;
            }
            if (!versionCirclesChanged) {
                consumeNodeChangeEvents(size);
                return;
            }
            LOG.info("Rebuild hash ring for realtime with cooldown, nodeChangeEvents size {}", size);
            TreeMap<Version, TreeMap<Integer, DiscoveryNode>> newCircles = new TreeMap<>();
            for (Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> entry : circles.entrySet()) {
                newCircles.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            circlesForRealtimeAD = newCircles;
            lastUpdateForRealtimeAD = clock.millis();
            LOG.info("Build version hash ring successfully");
            DiscoveryNode localNode = clusterService.localNode();
            String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
            boolean localNodeResolved = localNodeVersionKey != null && nodeVersions.containsKey(localNodeVersionKey);
            for (ModelState<ThresholdedRandomCutForest> modelState : cacheProvider.get().getAllModels()) {
                String modelId = modelState.getModelId();
                if (modelId == null) {
                    continue;
                }
                String routingKey = modelState.getEntity().map(Object::toString).orElse(modelId);
                Optional<DiscoveryNode> node = getOwningNodeWithSameLocalVersionForRealtime(routingKey);
                if (node.isPresent() && localNodeResolved && !node.get().getId().equals(localNodeVersionKey)) {
                    LOG.info(REMOVE_MODEL_MSG + " {}", modelId);
                    String configId = modelState.getConfigId() != null
                        ? modelState.getConfigId()
                        : SingleStreamModelIdMapper.getConfigIdForModelId(modelId);
                    cacheProvider.get().stopModel(modelState.getTenantId(), configId, modelId);
                    LOG.info("Stopped model [{}] on old owning node", modelId);
                } else if (node.isPresent() && !localNodeResolved) {
                    LOG.debug("Skip stale-model cleanup because local hash ring identity is unresolved for model [{}]", modelId);
                }
            }
            // It's possible that multiple threads add new event to nodeChangeEvents,
            // but this is the only place to consume/poll the event and there is only
            // one thread poll it as we are using buildHashRingSemaphore
            // to control only 1 thread build hash ring.
            consumeNodeChangeEvents(size);
        }
    }

    /**
     * Seed a single rebuild event when a build pass changes circles without a queued node-change
     * event, such as the non-delta snapshot path or deferred readiness retries. This preserves the
     * existing realtime rebuild gate while letting deferred retries wake up independently via
     * {@link #hasDueDeferredNodeProbe()}.
     *
     * @param versionCirclesChanged whether the current build pass changed circles
     */
    private void seedRealtimeRebuildEventIfNeeded(boolean versionCirclesChanged) {
        if (versionCirclesChanged && nodeChangeEvents.isEmpty()) {
            addNodeChangeEvent();
        }
    }

    /**
     * Check if it's eligible to rebuilt hash ring now.
     * It's eligible if:
     * 1. There is node change event not consumed, and
     * 2. Have passed cool down period from last hash ring update time.
     *
     * Check {@link org.opensearch.ad.settings.AnomalyDetectorSettings#AD_COOLDOWN_MINUTES} about
     * cool down settings.
     *
     * Why we need to wait for some cooldown period before rebuilding hash ring?
     *    This is for realtime detection. In realtime detection, we rely on hash ring to get
     *    owning node for RCF model partitions. It's stateless, that means we don't record
     *    which node is running RCF partition for the detector. That requires a stable hash
     *    ring. If hash ring changes, it's possible that the next job run will use a different
     *    node to run RCF partition. Then we need to restore model on the new node and clean up
     *    old model partitions on old node. That model migration between nodes may bring heavy
     *    load to cluster.
     *
     * @return true if it's eligible to rebuild hash ring
     */
    public boolean eligibleToRebuildCirclesForRealtimeAD() {
        // Check if there is any node change event
        if (nodeChangeEvents.isEmpty() && !hasPendingDeferredNodeProbe() && !circlesForRealtimeAD.isEmpty()) {
            return false;
        }

        // If realtime circles are still empty, rebuild immediately to avoid startup races
        // where membership arrives shortly after an empty initial build.
        if (circlesForRealtimeAD.isEmpty()) {
            return true;
        }

        // Check cooldown period
        if (clock.millis() - lastUpdateForRealtimeAD <= coolDownPeriodForRealtimeAD.getMillis()) {
            LOG.debug(COOLDOWN_MSG);
            return false;
        }
        return true;
    }

    /**
     * Get owning node with highest version circle.
     * @param modelId model id
     * @return owning node
     */
    public Optional<DiscoveryNode> getOwningNodeWithHighestVersion(String modelId) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> versionTreeMapEntry = circles.lastEntry();
        if (versionTreeMapEntry == null) {
            return Optional.empty();
        }
        TreeMap<Integer, DiscoveryNode> versionCircle = versionTreeMapEntry.getValue();
        Map.Entry<Integer, DiscoveryNode> entry = versionCircle.higherEntry(modelHash);
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(versionCircle.firstEntry())).map(x -> x.getValue());
    }

    /**
     * Get owning node with same version of local node.
     * @param modelId model id
     * @param function consumer function
     * @param listener action listener
     * @param <T> listener response type
     */
    public <T> void buildAndGetOwningNodeWithSameLocalVersion(
        String modelId,
        Consumer<Optional<DiscoveryNode>> function,
        ActionListener<T> listener
    ) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode localNode = clusterService.localNode();
            String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
            Version version = localNodeVersionKey != null && nodeVersions.containsKey(localNodeVersionKey)
                ? getVersion(localNodeVersionKey)
                : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameVersionDirectly(modelId, version, false);
            function.accept(owningNode);
        }, e -> listener.onFailure(e)));
    }

    public Optional<DiscoveryNode> getOwningNodeWithSameLocalVersionForRealtime(String modelId) {
        try {
            buildCirclesForRealtime();
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameLocalVersionForRealtimeDirectly(modelId);
            if (owningNode.isEmpty() && circlesForRealtimeAD.isEmpty() && hasPendingDeferredNodeProbe()) {
                // When startup has only deferred model nodes, retry immediately on the first realtime lookup
                // instead of waiting for the deferred backoff window to expire.
                clearDeferredNodeProbeStates(new HashSet<>(deferredNodeProbeStates.keySet()));
                buildCircles(ActionListener.wrap(r -> {}, e -> LOG.error("Failed forced realtime hash ring refresh", e)));
                owningNode = getOwningNodeWithSameLocalVersionForRealtimeDirectly(modelId);
            }
            LOG.debug("Owning node with same local version for realtime: {}", owningNode.orElse(null));
            return owningNode;
        } catch (Exception e) {
            LOG.error("Failed to get owning node with same local time series version", e);
            return Optional.empty();
        }
    }

    private Optional<DiscoveryNode> getOwningNodeWithSameLocalVersionForRealtimeDirectly(String modelId) {
        DiscoveryNode localNode = clusterService.localNode();
        String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
        Version version = localNodeVersionKey != null && nodeVersions.containsKey(localNodeVersionKey)
            ? getVersion(localNodeVersionKey)
            : Version.CURRENT;
        return getOwningNodeWithSameVersionDirectly(modelId, version, true);
    }

    private Optional<DiscoveryNode> getOwningNodeWithSameVersionDirectly(String modelId, Version version, boolean forRealtime) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        TreeMap<Integer, DiscoveryNode> versionCircle = forRealtime ? circlesForRealtimeAD.get(version) : circles.get(version);
        if (versionCircle != null) {
            Map.Entry<Integer, DiscoveryNode> entry = versionCircle.higherEntry(modelHash);
            return Optional.ofNullable(Optional.ofNullable(entry).orElse(versionCircle.firstEntry())).map(x -> x.getValue());
        }
        return Optional.empty();
    }

    public <T> void getNodesWithSameLocalVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(updated -> {
            DiscoveryNode localNode = clusterService.localNode();
            String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
            Version version = localNodeVersionKey != null && nodeVersions.containsKey(localNodeVersionKey)
                ? getVersion(localNodeVersionKey)
                : Version.CURRENT;
            Set<DiscoveryNode> nodes = getNodesWithSameVersion(version, false);
            if (shouldIncludeLocalNode(localNodeVersionKey) && !nodeVersions.containsKey(localNodeVersionKey)) {
                nodes.add(localNode);
            }
            // Make sure listener return in function
            function.accept(nodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    public DiscoveryNode[] getNodesWithSameLocalVersion() {
        DiscoveryNode localNode = clusterService.localNode();
        String localNodeVersionKey = resolveLocalNodeVersionKey(localNode);
        Version version = localNodeVersionKey != null && nodeVersions.containsKey(localNodeVersionKey)
            ? getVersion(localNodeVersionKey)
            : Version.CURRENT;
        Set<DiscoveryNode> nodes = getNodesWithSameVersion(version, false);
        // rebuild hash ring
        buildCirclesForRealtime();
        return nodes.toArray(new DiscoveryNode[0]);
    }

    public Set<DiscoveryNode> getNodesWithSameVersion(Version version, boolean forRealtime) {
        TreeMap<Integer, DiscoveryNode> circle = forRealtime ? circlesForRealtimeAD.get(version) : circles.get(version);
        Set<String> nodeIds = new HashSet<>();
        Set<DiscoveryNode> nodes = new HashSet<>();
        if (circle == null) {
            return nodes;
        }
        circle.entrySet().stream().forEach(e -> {
            DiscoveryNode discoveryNode = e.getValue();
            if (!nodeIds.contains(discoveryNode.getId())) {
                nodeIds.add(discoveryNode.getId());
                nodes.add(discoveryNode);
            }
        });
        return nodes;
    }

    /**
     * Get time series version.
     * @param nodeId node id
     * @return version
     */
    public Version getVersion(String nodeId) {
        TimeSeriesNodeInfo nodeInfo = nodeVersions.get(nodeId);
        return nodeInfo == null ? null : nodeInfo.getVersion();
    }

    /**
     *
     * Prefer node ID first to preserve single-tenant behavior.
     * Fall back to address key only when map data already uses that format.
     * 
     * @param localNode local node
     * @return local node version key
     */
    private String resolveLocalNodeVersionKey(DiscoveryNode localNode) {
        if (localNode == null) {
            return null;
        }

        String nodeIdKey = localNode.getId();
        if (nodeVersions.containsKey(nodeIdKey)) {
            return nodeIdKey;
        }

        // Multi-tenant: CloudMap nodes are keyed by ip:HTTP-port while localNode.getAddress()
        // uses the transport port. If there is exactly one ring node with the same IP, use that
        // key as the local hash-ring identity. Ambiguous same-IP matches are ignored.
        TransportAddress address = localNode.getAddress();
        if (address != null) {
            String key = findUniqueNodeVersionKeyByIp(TransportUtil.extractIp(address));
            if (key != null) {
                return key;
            }
        }

        // Preserve the existing bootstrap behavior when no local key has been recorded.
        return nodeIdKey;
    }

    private String findUniqueNodeVersionKeyByIp(String ipAddress) {
        String matchedKey = null;
        for (String key : nodeVersions.keySet()) {
            if (!key.split(":")[0].equals(ipAddress)) {
                continue;
            }
            if (matchedKey != null) {
                LOG.debug("Local hash ring identity is ambiguous for IP [{}]; matched [{}] and [{}]", ipAddress, matchedKey, key);
                return null;
            }
            matchedKey = key;
        }
        return matchedKey;
    }

    /**
     * Decide whether to force-add local node before calling nodesInfo().
     *
     * Multi-tenant: local UUID is no longer force-added when it is not part of
     * CloudMap/eligible membership, so repeated local-add attempts stop.
     *
     * Single-tenant: behavior remains the same because local node ID exists in
     * eligible membership.
     *
     * @param localNodeVersionKey resolved local key (node id or ip:port)
     * @return true if local key exists in current eligible membership
     */
    private boolean shouldIncludeLocalNode(String localNodeVersionKey) {
        return localNodeVersionKey != null && nodeFilter.nodeExists(localNodeVersionKey);
    }

    private DiscoveryNode[] getEligibleDataNodesForHashRing() {
        DiscoveryNode[] eligibleNodes = nodeFilter.getEligibleDataNodes();
        if (eligibleNodes != null && eligibleNodes.length > 0) {
            return eligibleNodes;
        }

        DiscoveryNode localModelNode = getSingleLocalCoordinatorModelNode();
        if (localModelNode == null) {
            return new DiscoveryNode[0];
        }
        return new DiscoveryNode[] { localModelNode };
    }

    private boolean shouldBootstrapSingleLocalCoordinatorModelNode() {
        return getSingleLocalCoordinatorModelNode() != null && circlesForRealtimeAD.isEmpty();
    }

    private DiscoveryNode getSingleLocalCoordinatorModelNode() {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE) || !roles.contains(TimeSeriesSettings.MODEL_ROLE)) {
            return null;
        }

        DiscoveryNode localNode = clusterService.localNode();
        if (localNode == null || localNode.getAddress() == null) {
            return null;
        }
        String localIp = TransportUtil.extractIp(localNode.getAddress());
        if (localIp == null || localIp.isBlank()) {
            return null;
        }
        return TransportUtil.createDiscoveryNodeFromIp(localIp, opensearchPort);
    }

    /**
     * Get node by transport address.
     * If transport address is null, return local node; otherwise, filter current eligible data nodes
     * with IP address. If no node found, will return Optional.empty()
     *
     * @param address transport address
     * @return discovery node
     */
    public Optional<DiscoveryNode> getNodeByAddress(TransportAddress address) {
        if (address == null) {
            // If remote address of transport request is null, that means remote node is local node.
            return Optional.of(clusterService.localNode());
        }
        String ipAddress = getIpAddress(address);
        DiscoveryNode[] allNodes = nodeFilter.getEligibleDataNodes();

        // Can't handle this edge case for BWC of AD1.0: mixed cluster with AD1.0 and Version after 1.1.
        // Start multiple OpenSearch processes on same IP, some run AD 1.0, some run new AD
        // on or after 1.1. As we ignore port number in transport address, just look for node
        // with IP like "127.0.0.1", so it's possible that we get wrong node as all nodes have
        // same IP.
        for (DiscoveryNode node : allNodes) {
            if (getIpAddress(node.getAddress()).equals(ipAddress)) {
                return Optional.ofNullable(node);
            }
        }
        return Optional.empty();
    }

    private String getIpAddress(TransportAddress address) {
        return TransportUtil.extractIp(address);
    }

    /**
     * Get all eligible data nodes whose time series versions are known in hash ring.
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAllEligibleDataNodesWithKnownVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
            List<DiscoveryNode> allNodes = new ArrayList<>();
            for (DiscoveryNode node : eligibleDataNodes) {
                if (nodeVersions.containsKey(node.getId())) {
                    allNodes.add(node);
                }
            }
            // Make sure listener return in function
            function.accept(allNodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    /**
     * Put node change events in node change event queue. Will poll event from this queue when rebuild hash ring
     * for realtime task.
     * We track all node change events in case some race condition happen and we miss adding some node to hash
     * ring.
     */
    public void addNodeChangeEvent() {
        this.nodeChangeEvents.add(true);
    }

    /**
     * Record a failed readiness probe for eligible nodes that were discovered from membership but were not admitted
     * into {@code nodeVersions}/{@code circles} during the latest {@code nodesInfo()} refresh.
     *
     * These nodes remain eligible candidates, so we keep retry state and apply backoff before probing them again on a
     * later rebuild.
     *
     * @param nodeIds eligible node IDs that should be retried later
     */
    private void recordDeferredNodeProbeFailures(Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return;
        }
        long now = clock.millis();
        for (String nodeId : nodeIds) {
            deferredNodeProbeStates.compute(nodeId, (id, state) -> {
                DeferredNodeProbeState nextState = state == null ? new DeferredNodeProbeState() : state;
                nextState.recordFailure(now);
                return nextState;
            });
        }
    }

    /**
     * Clear deferred probe state once a node is either admitted successfully or explicitly removed from membership.
     *
     * After clearing, the node no longer participates in deferred retry bookkeeping.
     *
     * @param nodeIds node IDs whose deferred retry state should be removed
     */
    private void clearDeferredNodeProbeStates(Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return;
        }
        for (String nodeId : nodeIds) {
            deferredNodeProbeStates.remove(nodeId);
        }
    }

    /**
     * Remove retry state for nodes that are no longer part of the current eligible membership snapshot.
     *
     * This is used by the non-delta rebuild path, where we only have the latest eligible-node snapshot instead of an
     * explicit removed-node list. Deferred state must be preserved for still-eligible nodes so they can be retried
     * later, but it should be dropped for nodes that have already left membership to avoid useless probes.
     *
     * @param eligibleNodeIds node IDs from the latest eligible membership snapshot
     */
    private void pruneDeferredNodeProbeStates(Set<String> eligibleNodeIds) {
        if (eligibleNodeIds == null) {
            return;
        }
        for (String nodeId : new HashSet<>(deferredNodeProbeStates.keySet())) {
            if (!eligibleNodeIds.contains(nodeId)) {
                deferredNodeProbeStates.remove(nodeId);
            }
        }
    }

    /**
     * Return the subset of candidate nodes whose deferred retry backoff has expired.
     *
     * Nodes with no deferred state are treated as immediately due, which lets newly discovered nodes probe right away.
     * Nodes with deferred state are retried only when their next scheduled probe time has arrived.
     *
     * @param candidateNodeIds nodes discovered from current membership that may need a {@code nodesInfo()} refresh
     * @return candidate node IDs that should be probed in this rebuild
     */
    private Set<String> getDueNodeIds(Set<String> candidateNodeIds) {
        long now = clock.millis();
        return candidateNodeIds.stream().filter(nodeId -> {
            DeferredNodeProbeState state = deferredNodeProbeStates.get(nodeId);
            return state == null || state.isDue(now);
        }).collect(Collectors.toSet());
    }

    /**
     * @return {@code true} when at least one eligible node is waiting for a deferred readiness retry
     */
    private boolean hasPendingDeferredNodeProbe() {
        return !deferredNodeProbeStates.isEmpty();
    }

    /**
     * @return {@code true} when any deferred node has reached its next scheduled retry time
     */
    private boolean hasDueDeferredNodeProbe() {
        if (deferredNodeProbeStates.isEmpty()) {
            return false;
        }
        long now = clock.millis();
        return deferredNodeProbeStates.values().stream().anyMatch(state -> state.isDue(now));
    }

    private void consumeNodeChangeEvents(int size) {
        while (size-- > 0) {
            Boolean poll = nodeChangeEvents.poll();
            if (poll == null) {
                break;
            }
        }
    }

    private static final class DeferredNodeProbeState {
        private int attemptCount;
        private long nextProbeAtMillis;

        /**
         * Record a failed readiness probe and calculate the next probe time in
         * {@code nextProbeAtMillis} from {@code DEFERRED_NODE_RETRY_DELAYS_MILLIS}.
         *
         * A failed probe does not retire the node. The node stays deferred until it is
         * either admitted successfully into {@code nodeVersions}/{@code circles} or removed
         * from eligible membership. The retry cadence is controlled by
         * {@code DEFERRED_NODE_RETRY_DELAYS_MILLIS}: 20s, 60s, 300s, 600s, then 600s
         * repeatedly.
         *
         * Time passing alone does not retry the node. A later hash-ring build pass must run.
         * A retry happens only when a later {@code buildCircles(...)} or
         * {@code buildCirclesForRealtime()} pass runs, the node is considered due by
         * {@code hasDueDeferredNodeProbe()} or {@code getDueNodeIds(...)}, and
         * {@code dataAccess.nodesInfo(...)} is sent again for that node.
         *
         * Typical triggers for that later build pass are:
         * - cluster membership changes handled through {@code ClusterEventListener} or
         *   {@code ClusterMembershipReader}
         * - realtime call sites that invoke {@code buildCirclesForRealtime()}
         * - connection-failure handling that invokes {@code buildCirclesForRealtime()}
         *   when a node disappears
         *
         * @param now current time in milliseconds
         */
        private void recordFailure(long now) {
            nextProbeAtMillis = now + DEFERRED_NODE_RETRY_DELAYS_MILLIS[Math
                .min(attemptCount, DEFERRED_NODE_RETRY_DELAYS_MILLIS.length - 1)];
            attemptCount++;
        }

        private boolean isDue(long now) {
            return now >= nextProbeAtMillis;
        }
    }
}
