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

package org.opensearch.timeseries.indices;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.rest.handler.TimeSeriesFunction;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public abstract class TimeSeriesIndices implements LocalNodeClusterManagerListener {
    private static final Logger logger = LogManager.getLogger(TimeSeriesIndices.class);

    protected class IndexState {
        // keep track of whether the mapping version is up-to-date
        public Boolean mappingUpToDate;
        // keep track of whether the setting needs to change
        public Boolean settingUpToDate;
        // record schema version reading from the mapping file
        public Integer schemaVersion;

        public IndexState(String mappingFile) {
            this.mappingUpToDate = false;
            this.settingUpToDate = false;
            this.schemaVersion = TimeSeriesIndices.parseSchemaVersion(mappingFile);
        }
    }

    /**
     * Alias exists or not
     * @param clusterServiceAccessor Cluster service
     * @param alias Alias name
     * @return true if the alias exists
     */
    public static boolean doesAliasExists(ClusterService clusterServiceAccessor, String alias) {
        return clusterServiceAccessor.state().metadata().hasAlias(alias);
    }

    /**
     * Index exists or not
     * @param clusterServiceAccessor Cluster service
     * @param name Index name
     * @return true if the index exists
     */
    public static boolean doesIndexExists(ClusterService clusterServiceAccessor, String name) {
        return clusterServiceAccessor.state().getRoutingTable().hasIndex(name);
    }

    public static Integer parseSchemaVersion(String mapping) {
        try {
            XContentParser xcp = XContentType.JSON
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mapping);

            while (!xcp.isClosed()) {
                Token token = xcp.currentToken();
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != TimeSeriesIndices.META) {
                        xcp.nextToken();
                        xcp.skipChildren();
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (xcp.currentName().equals(TimeSeriesIndices.SCHEMA_VERSION)) {

                                Integer version = xcp.intValue();
                                if (version < 0) {
                                    version = CommonValue.NO_SCHEMA_VERSION;
                                }
                                return version;
                            } else {
                                xcp.nextToken();
                            }
                        }

                    }
                }
                xcp.nextToken();
            }
            return CommonValue.NO_SCHEMA_VERSION;
        } catch (Exception e) {
            // since this method is called in the constructor that is called by TimeSeriesAnalyticsPlugin.createComponents,
            // we cannot throw checked exception
            throw new RuntimeException(e);
        }
    }

    protected static Integer getIntegerSetting(GetSettingsResponse settingsResponse, String settingKey) {
        Integer value = null;
        Iterator<Settings> iter = settingsResponse.getIndexToSettings().valuesIt();
        while (iter.hasNext()) {
            Settings settings = iter.next();
            value = settings.getAsInt(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }

    protected static String getStringSetting(GetSettingsResponse settingsResponse, String settingKey) {
        String value = null;
        Iterator<Settings> iter = settingsResponse.getIndexToSettings().valuesIt();
        while (iter.hasNext()) {
            Settings settings = iter.next();
            value = settings.get(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }

    // minimum shards of the job index
    public static int minJobIndexReplicas = 1;
    // maximum shards of the job index
    public static int maxJobIndexReplicas = 20;
    // package private for testing
    public static final String META = "_meta";
    public static final String SCHEMA_VERSION = "schema_version";

    protected ClusterService clusterService;
    protected final Client client;
    protected final AdminClient adminClient;
    protected final ThreadPool threadPool;
    protected DiscoveryNodeFilterer nodeFilter;
    // index settings
    protected final Settings settings;
    // don't retry updating endlessly. Can be annoying if there are too many exception logs.
    protected final int maxUpdateRunningTimes;

    protected TimeSeriesIndices(Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            Settings settings,
            DiscoveryNodeFilterer nodeFilter,
            int maxUpdateRunningTimes) {
        this.client = client;
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.nodeFilter = nodeFilter;
        this.settings = Settings.builder().put("index.hidden", true).build();
        this.maxUpdateRunningTimes = maxUpdateRunningTimes;
    }

    public boolean doesIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }

    protected void choosePrimaryShards(CreateIndexRequest request, boolean hiddenIndex) {
        request
            .settings(
                Settings
                    .builder()
                    // put 1 primary shards per hot node if possible
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, getNumberOfPrimaryShards())
                    // 1 replica for better search performance and fail-over
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.hidden", hiddenIndex)
            );
    }

    protected abstract int getNumberOfPrimaryShards();

    protected void deleteOldHistoryIndices(String indexPattern, TimeValue historyRetentionPeriod) {
        Set<String> candidates = new HashSet<String>();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest()
            .clear()
            .indices(indexPattern)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand());

        adminClient.cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            String latestToDelete = null;
            long latest = Long.MIN_VALUE;
            for (ObjectCursor<IndexMetadata> cursor : clusterStateResponse.getState().metadata().indices().values()) {
                IndexMetadata indexMetaData = cursor.value;
                long creationTime = indexMetaData.getCreationDate();

                if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis()) {
                    String indexName = indexMetaData.getIndex().getName();
                    candidates.add(indexName);
                    if (latest < creationTime) {
                        latest = creationTime;
                        latestToDelete = indexName;
                    }
                }
            }

            if (candidates.size() > 1) {
                // delete all indices except the last one because the last one may contain docs newer than the retention period
                candidates.remove(latestToDelete);
                String[] toDelete = candidates.toArray(Strings.EMPTY_ARRAY);
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(toDelete);
                adminClient.indices().delete(deleteIndexRequest, ActionListener.wrap(deleteIndexResponse -> {
                    if (!deleteIndexResponse.isAcknowledged()) {
                        logger
                            .error(
                                "Could not delete one or more result indices: {}. Retrying one by one.",
                                Arrays.toString(toDelete)
                            );
                        deleteIndexIteration(toDelete);
                    } else {
                        logger.info("Succeeded in deleting expired result indices: {}.", Arrays.toString(toDelete));
                    }
                }, exception -> {
                    logger.error("Failed to delete expired result indices: {}.", Arrays.toString(toDelete));
                    deleteIndexIteration(toDelete);
                }));
            }
        }, exception -> { logger.error("Fail to delete result indices", exception); }));
    }

    protected void deleteIndexIteration(String[] toDelete) {
        for (String index : toDelete) {
            DeleteIndexRequest singleDeleteRequest = new DeleteIndexRequest(index);
            adminClient.indices().delete(singleDeleteRequest, ActionListener.wrap(singleDeleteResponse -> {
                if (!singleDeleteResponse.isAcknowledged()) {
                    logger.error("Retrying deleting {} does not succeed.", index);
                }
            }, exception -> {
                if (exception instanceof IndexNotFoundException) {
                    logger.info("{} was already deleted.", index);
                } else {
                    logger.error(new ParameterizedMessage("Retrying deleting {} does not succeed.", index), exception);
                }
            }));
        }
    }

    @SuppressWarnings("unchecked")
    protected void shouldUpdateConcreteIndex(String concreteIndex, Integer newVersion, ActionListener<Boolean> thenDo) {
        IndexMetadata indexMeataData = clusterService.state().getMetadata().indices().get(concreteIndex);
        if (indexMeataData == null) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = CommonValue.NO_SCHEMA_VERSION;

        Map<String, Object> indexMapping = indexMeataData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(TimeSeriesIndices.META);
        if (meta != null && meta instanceof Map) {
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        thenDo.onResponse(newVersion > oldVersion);
    }

    protected void updateJobIndexSettingIfNecessary(String indexName, IndexState jobIndexState, ActionListener<Void> listener) {
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
            .indices(indexName)
            .names(
                new String[] {
                    IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                    IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                    IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS }
            );
        client.execute(GetSettingsAction.INSTANCE, getSettingsRequest, ActionListener.wrap(settingResponse -> {
            // auto expand setting is a range string like "1-all"
            String autoExpandReplica = getStringSetting(settingResponse, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS);
            // if the auto expand setting is already there, return immediately
            if (autoExpandReplica != null) {
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
                return;
            }
            Integer primaryShardsNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_SHARDS);
            Integer replicaNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            if (primaryShardsNumber == null || replicaNumber == null) {
                logger
                    .error(
                        new ParameterizedMessage(
                            "Fail to find AD job index's primary or replica shard number: primary [{}], replica [{}]",
                            primaryShardsNumber,
                            replicaNumber
                        )
                    );
                // don't throw exception as we don't know how to handle it and retry next time
                listener.onResponse(null);
                return;
            }
            // at least minJobIndexReplicas
            // at most maxJobIndexReplicas / primaryShardsNumber replicas.
            // For example, if we have 2 primary shards, since the max number of shards are maxJobIndexReplicas (20),
            // we will use 20 / 2 = 10 replicas as the upper bound of replica.
            int maxExpectedReplicas = Math.max(TimeSeriesIndices.maxJobIndexReplicas / primaryShardsNumber, TimeSeriesIndices.minJobIndexReplicas);
            Settings updatedSettings = Settings
                .builder()
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, TimeSeriesIndices.minJobIndexReplicas + "-" + maxExpectedReplicas)
                .build();
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName)
                .settings(updatedSettings);
            client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(response -> {
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
            }, listener::onFailure));
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                // new index will be created with auto expand replica setting
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Create config index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initConfigIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesConfigIndexExist()) {
            initConfigIndex(actionListener);
        }
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(CommonName.CONFIG_INDEX)
            .mapping(getConfigMappings(), XContentType.JSON)
            .settings(settings);
        adminClient.indices().create(request, actionListener);
    }

    /**
     * Config index exist or not.
     *
     * @return true if config index exists
     */
    public boolean doesConfigIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(CommonName.CONFIG_INDEX);
    }

    /**
     * Job index exist or not.
     *
     * @return true if anomaly detector job index exists
     */
    public boolean doesJobIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(CommonName.JOB_INDEX);
    }

    /**
     * Get config index mapping in json format.
     *
     * @return config index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getConfigMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(TimeSeriesSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get job index mapping in json format.
     *
     * @return job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getJobMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(TimeSeriesSettings.ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Createjob index.
     *
     * @param actionListener action called after create index
     */
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(CommonName.JOB_INDEX)
                .mapping(getJobMappings(), XContentType.JSON);
            request
                .settings(
                    Settings
                        .builder()
                        // AD job index is small. 1 primary shard is enough
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        // Job scheduler puts both primary and replica shards in the
                        // hash ring. Auto-expand the number of replicas based on the
                        // number of data nodes (up to 20) in the cluster so that each node can
                        // become a coordinating node. This is useful when customers
                        // scale out their cluster so that we can do adaptive scaling
                        // accordingly.
                        // At least 1 replica for fail-over.
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, minJobIndexReplicas + "-" + maxJobIndexReplicas)
                        .put("index.hidden", true)
                );
            adminClient.indices().create(request, actionListener);
        } catch (IOException e) {
            logger.error("Fail to init AD job index", e);
            actionListener.onFailure(e);
        }
    }

    public <T> void validateCustomResultIndexAndExecute(String resultIndex, TimeSeriesFunction function, ActionListener<T> listener) {
        try {
            if (!isValidResultIndexMapping(resultIndex)) {
                logger.warn("Can't create detector with custom result index {} as its mapping is invalid", resultIndex);
                listener.onFailure(new IllegalArgumentException(CommonMessages.INVALID_RESULT_INDEX_MAPPING + resultIndex));
                return;
            }

            IndexRequest indexRequest = createDummyIndexRequest(resultIndex);

            // User may have no write permission on custom result index. Talked with security plugin team, seems no easy way to verify
            // if user has write permission. So just tried to write and delete a dummy forecast result to verify.
            client.index(indexRequest, ActionListener.wrap(response -> {
                logger.debug("Successfully wrote dummy result to result index {}", resultIndex);
                client.delete(createDummyDeleteRequest(resultIndex), ActionListener.wrap(deleteResponse -> {
                    logger.debug("Successfully deleted dummy result from result index {}", resultIndex);
                    function.execute();
                }, ex -> {
                    logger.error("Failed to delete dummy result from result index " + resultIndex, ex);
                    listener.onFailure(ex);
                }));
            }, exception -> {
                logger.error("Failed to write dummy result to result index " + resultIndex, exception);
                listener.onFailure(exception);
            }));
        } catch (Exception e) {
            logger.error("Failed to validate custom result index " + resultIndex, e);
            listener.onFailure(e);
        }
    }

    protected abstract IndexRequest createDummyIndexRequest(String resultIndex) throws IOException;

    protected abstract DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException;

    public abstract boolean isValidResultIndexMapping(String resultIndex);

    public abstract <T> void initCustomResultIndexAndExecute(String resultIndex, TimeSeriesFunction function, ActionListener<T> listener);

    public abstract boolean doesCheckpointIndexExist();

    public abstract void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener);

    public abstract int getSchemaVersion(TimeSeriesIndex index);

    public abstract boolean doesDefaultResultIndexExist();

    public abstract void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException;
}
