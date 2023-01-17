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

package org.opensearch.forecast.indices;

import static org.opensearch.forecast.constant.ForecastCommonMessages.CAN_NOT_FIND_RESULT_INDEX;
import static org.opensearch.forecast.constant.ForecastCommonName.DUMMY_FORECAST_RESULT_ID;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_RETENTION_PERIOD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_STATE_INDEX_MAPPING_FILE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULTS_INDEX_MAPPING_FILE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_INDEX_MAPPING_FILE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_PRIMARY_SHARDS;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.indices.TimeSeriesIndices;
import org.opensearch.timeseries.rest.handler.TimeSeriesFunction;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class ForecastIndices extends TimeSeriesIndices  {
    private static final Logger logger = LogManager.getLogger(ForecastIndices.class);

    // The index name pattern to query all the forecast result history indices
    public static final String FORECAST_RESULT_HISTORY_INDEX_PATTERN = "<opensearch-forecast-results-history-{now/d}-1>";

    // The index name pattern to query all forecast results, history and current forecast results
    public static final String ALL_FORECAST_RESULTS_INDEX_PATTERN = "opensearch-forecast-results*";

    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;
    private volatile TimeValue historyRetentionPeriod;

    private Scheduler.Cancellable scheduledRollover = null;

    private int maxPrimaryShards;
    // keep track of whether the mapping version is up-to-date
    private EnumMap<ForecastIndex, IndexState> indexStates;
    // whether all index have the correct mappings
    private boolean allMappingUpdated;
    // whether all index settings are updated
    private boolean allSettingUpdated;
    // we only want one update at a time
    private final AtomicBoolean updateRunning;
    // the number of times updates run
    private int updateRunningTimes;
    // result index mapping to validate custom index
    private Map<String, Object> FORECAST_RESULT_FIELD_CONFIGS;

    /**
     * Constructor function
     *
     * @param client         OS client supports administrative actions
     * @param clusterService OS cluster service
     * @param threadPool     OS thread pool
     * @param settings       OS cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host forecast indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     */
    public ForecastIndices(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes
    ) {
        super(client, clusterService, threadPool, settings, nodeFilter, maxUpdateRunningTimes);
        this.historyRolloverPeriod = FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings);
        this.historyRetentionPeriod = FORECAST_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.maxPrimaryShards = FORECAST_MAX_PRIMARY_SHARDS.get(settings);
        this.indexStates = new EnumMap<ForecastIndex, IndexState>(ForecastIndex.class);

        this.allMappingUpdated = false;
        this.allSettingUpdated = false;
        this.updateRunning = new AtomicBoolean(false);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_RETENTION_PERIOD, it -> { historyRetentionPeriod = it; });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);

        this.updateRunningTimes = 0;

        this.FORECAST_RESULT_FIELD_CONFIGS = null;
    }

    private void initResultMapping() throws IOException {
        if (FORECAST_RESULT_FIELD_CONFIGS != null) {
            // we have already initiated the field
            return;
        }
        String resultMapping = getForecastResultMappings();

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
        Object properties = asMap.get(CommonName.PROPERTIES);
        if (properties instanceof Map) {
            FORECAST_RESULT_FIELD_CONFIGS = (Map<String, Object>) properties;
        } else {
            logger.error("Fail to read result mapping file.");
        }
    }

    /**
     * Get forecast result index mapping json content.
     *
     * @return forecast result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getForecastResultMappings() throws IOException {
        URL url = ForecastIndices.class.getClassLoader().getResource(FORECAST_RESULTS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get forecaster state index mapping json content.
     *
     * @return forecaster state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getForecastStateMappings() throws IOException {
        URL url = ForecastIndices.class.getClassLoader().getResource(FORECAST_STATE_INDEX_MAPPING_FILE);
        String forecastStateMappings = Resources.toString(url, Charsets.UTF_8);
        String forecasterIndexMappings = getConfigMappings();
        forecasterIndexMappings = forecasterIndexMappings
            .substring(forecasterIndexMappings.indexOf("\"properties\""), forecasterIndexMappings.lastIndexOf("}"));
        return forecasterIndexMappings.replace("FORECASTER_INDEX_MAPPING_PLACE_HOLDER", forecasterIndexMappings);
    }

    /**
     * Get checkpoint index mapping json content.
     *
     * @return checkpoint index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getCheckpointMappings() throws IOException {
        URL url = ForecastIndices.class.getClassLoader().getResource(FORECAST_CHECKPOINT_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * default forecaster result index exist or not.
     *
     * @return true if default forecaster result index exists
     */
    public boolean doesDefaultForecastResultIndexExist() {
        return clusterService.state().metadata().hasAlias(ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
    }

    @Override
    public <T> void initCustomResultIndexAndExecute(String resultIndex, TimeSeriesFunction function, ActionListener<T> listener) {
        try {
            if (!doesIndexExist(resultIndex)) {
                initCustomForecastResultIndexDirectly(resultIndex, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        logger.info("Successfully created forecast result index {}", resultIndex);
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        String error = "Creating forecast result index with mappings call not acknowledged: " + resultIndex;
                        logger.error(error);
                        listener.onFailure(new EndRunException(error, true));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        logger.error("Failed to create forecast result index " + resultIndex, exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                validateCustomResultIndexAndExecute(resultIndex, function, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to create custom result index " + resultIndex, e);
            listener.onFailure(e);
        }
    }

    public <T> void validateCustomIndexForBackendJob(
        String resultIndex,
        String securityLogId,
        String user,
        List<String> roles,
        TimeSeriesFunction function,
        ActionListener<T> listener
    ) {
        if (!doesIndexExist(resultIndex)) {
            listener.onFailure(new EndRunException(CAN_NOT_FIND_RESULT_INDEX + resultIndex, true));
            return;
        }
        if (!isValidResultIndexMapping(resultIndex)) {
            listener.onFailure(new EndRunException("Result index mapping is not correct", true));
            return;
        }
        try (InjectSecurity injectSecurity = new InjectSecurity(securityLogId, settings, client.threadPool().getThreadContext())) {
            injectSecurity.inject(user, roles);
            ActionListener<T> wrappedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
                injectSecurity.close();
                listener.onFailure(e);
            });
            validateCustomResultIndexAndExecute(resultIndex, () -> {
                injectSecurity.close();
                function.execute();
            }, wrappedListener);
        } catch (Exception e) {
            logger.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }

    /**
     * Check if custom result index has correct index mapping.
     * @param resultIndex result index
     * @return true if result index mapping is valid
     */
    @Override
    public boolean isValidResultIndexMapping(String resultIndex) {
        try {
            initResultMapping();
            if (FORECAST_RESULT_FIELD_CONFIGS == null) {
                // failed to populate the field
                return false;
            }
            IndexMetadata indexMetadata = clusterService.state().metadata().index(resultIndex);
            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            String propertyName = CommonName.PROPERTIES;
            if (!indexMapping.containsKey(propertyName) || !(indexMapping.get(propertyName) instanceof LinkedHashMap)) {
                return false;
            }
            LinkedHashMap<String, Object> mapping = (LinkedHashMap<String, Object>) indexMapping.get(propertyName);

            boolean correctResultIndexMapping = true;

            for (String fieldName : FORECAST_RESULT_FIELD_CONFIGS.keySet()) {
                Object defaultSchema = FORECAST_RESULT_FIELD_CONFIGS.get(fieldName);
                // the field might be a map or map of map
                // example: map: {type=date, format=strict_date_time||epoch_millis}
                // map of map: {type=nested, properties={likelihood={type=double}, value_list={type=nested, properties={data={type=double},
                // feature_id={type=keyword}}}}}
                // if it is a map of map, Object.equals can compare them regardless of order
                if (!mapping.containsKey(fieldName) || !defaultSchema.equals(mapping.get(fieldName))) {
                    correctResultIndexMapping = false;
                    break;
                }
            }
            return correctResultIndexMapping;
        } catch (Exception e) {
            logger.error("Failed to validate result index mapping for index " + resultIndex, e);
            return false;
        }

    }

    /**
     * Forecast state index exist or not.
     *
     * @return true if forecast state index exists
     */
    public boolean doesForecasterStateIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(ForecastCommonName.FORECAST_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    @Override
    public boolean doesCheckpointIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME);
    }

    private ActionListener<CreateIndexResponse> markMappingUpToDate(ForecastIndex index, ActionListener<CreateIndexResponse> followingListener) {
        return ActionListener.wrap(createdResponse -> {
            if (createdResponse.isAcknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping()));
                if (Boolean.FALSE.equals(indexStatetate.mappingUpToDate)) {
                    indexStatetate.mappingUpToDate = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
            followingListener.onResponse(createdResponse);
        }, exception -> followingListener.onFailure(exception));
    }

    /**
     * Create forecast result index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link ForecastIndices#getForecastResultMappings}
     */
    public void initDefaultForecastResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesDefaultForecastResultIndexExist()) {
            initDefaultForecastIndexDirectly(actionListener);
        }
    }

    @Override
    protected int getNumberOfPrimaryShards() {
        return Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards);
    }

    /**
     * Create forecast result index without checking exist or not.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link ForecastIndices#getForecastResultMappings}
     */
    public void initDefaultForecastIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        initForecastResultIndexDirectly(FORECAST_RESULT_HISTORY_INDEX_PATTERN, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS, false, actionListener);
    }

    public void initCustomForecastResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener)
        throws IOException {
        initForecastResultIndexDirectly(resultIndex, null, false, actionListener);
    }

    private void initForecastResultIndexDirectly(
        String resultIndex,
        String alias,
        boolean hiddenIndex,
        ActionListener<CreateIndexResponse> actionListener
    ) throws IOException {
        String mapping = getForecastResultMappings();
        CreateIndexRequest request = new CreateIndexRequest(resultIndex).mapping(mapping, XContentType.JSON);
        if (alias != null) {
            request.alias(new Alias(ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS));
        }
        choosePrimaryShards(request, hiddenIndex);
        if (FORECAST_RESULT_HISTORY_INDEX_PATTERN.equals(resultIndex)) {
            adminClient.indices().create(request, markMappingUpToDate(ForecastIndex.RESULT, actionListener));
        } else {
            adminClient.indices().create(request, actionListener);
        }
    }

    /**
     * Create the state index.
     *
     * @param actionListener action called after create index
     */
    public void initForecastStateIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(ForecastCommonName.FORECAST_STATE_INDEX)
                .mapping(getForecastStateMappings(), XContentType.JSON)
                .settings(settings);
            adminClient.indices().create(request, markMappingUpToDate(ForecastIndex.STATE, actionListener));
        } catch (IOException e) {
            logger.error("Fail to init AD detection state index", e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Create the checkpoint index.
     *
     * @param actionListener action called after create index
     * @throws ForecastEndRunException ForecastEndRunException due to failure to get mapping
     */
    @Override
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        CreateIndexRequest request = new CreateIndexRequest(ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME).mapping(mapping, XContentType.JSON);
        choosePrimaryShards(request, true);
        adminClient.indices().create(request, markMappingUpToDate(ForecastIndex.CHECKPOINT, actionListener));
    }

    @Override
    public void onClusterManager() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAndDeleteHistoryIndex();

            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        } catch (Exception e) {
            // This should be run on cluster startup
            logger.error("Error rollover AD result indices. " + "Can't rollover AD result until clusterManager node is restarted.", e);
        }
    }

    @Override
    public void offClusterManager() {
        if (scheduledRollover != null) {
            scheduledRollover.cancel();
        }
    }

    private String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    private void rescheduleRollover() {
        if (clusterService.state().getNodes().isLocalNodeElectedClusterManager()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    void rolloverAndDeleteHistoryIndex() {
        if (!doesDefaultForecastResultIndexExist()) {
            return;
        }

        // We have to pass null for newIndexName in order to get OpenSearch to increment the index count.
        RolloverRequest rollOverRequest = new RolloverRequest(ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS, null);
        String forecastResultMapping = null;
        try {
            forecastResultMapping = getForecastResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over forecast result index, as can't get forecast result index mapping");
            return;
        }
        CreateIndexRequest createRequest = rollOverRequest.getCreateIndexRequest();

        createRequest.index(FORECAST_RESULT_HISTORY_INDEX_PATTERN).mapping(forecastResultMapping, XContentType.JSON);

        choosePrimaryShards(createRequest, false);

        rollOverRequest.addMaxIndexDocsCondition(historyMaxDocs * getNumberOfPrimaryShards());
        adminClient.indices().rolloverIndex(rollOverRequest, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger
                    .warn("{} not rolled over. Conditions were: {}", ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS, response.getConditionStatus());
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(ForecastIndex.RESULT, k -> new IndexState(k.getMapping()));
                indexStatetate.mappingUpToDate = true;
                logger.info("{} rolled over. Conditions were: {}", ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS, response.getConditionStatus());
                deleteOldHistoryIndices(ALL_FORECAST_RESULTS_INDEX_PATTERN, historyRetentionPeriod);
            }
        }, exception -> { logger.error("Fail to roll over result index", exception); }));
    }



    public void update() {
        if ((allMappingUpdated && allSettingUpdated) || updateRunningTimes >= maxUpdateRunningTimes || updateRunning.get()) {
            return;
        }
        updateRunning.set(true);
        updateRunningTimes++;

        // set updateRunning to false when both updateMappingIfNecessary and updateSettingIfNecessary
        // stop running
        final GroupedActionListener<Void> groupListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> updateRunning.set(false), exception -> {
                updateRunning.set(false);
                logger.error("Fail to update AD indices", exception);
            }),
            // 2 since we need both updateMappingIfNecessary and updateSettingIfNecessary to return
            // before setting updateRunning to false
            2
        );

        updateMappingIfNecessary(groupListeneer);
        updateSettingIfNecessary(groupListeneer);
    }

    private void updateSettingIfNecessary(GroupedActionListener<Void> delegateListeneer) {
        if (allSettingUpdated) {
            delegateListeneer.onResponse(null);
            return;
        }

        List<ForecastIndex> updates = new ArrayList<>();
        for (ForecastIndex index : ForecastIndex.values()) {
            Boolean updated = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).settingUpToDate;
            if (Boolean.FALSE.equals(updated)) {
                updates.add(index);
            }
        }
        if (updates.size() == 0) {
            allSettingUpdated = true;
            delegateListeneer.onResponse(null);
            return;
        }

        final GroupedActionListener<Void> conglomerateListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> delegateListeneer.onResponse(null), exception -> {
                delegateListeneer.onResponse(null);
                logger.error("Fail to update AD indices' mappings", exception);
            }),
            updates.size()
        );
        for (ForecastIndex forecastIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s setting", forecastIndex.getIndexName()));
            switch (forecastIndex) {
                case JOB:
                    updateJobIndexSettingIfNecessary(ForecastIndex.JOB.getIndexName(), indexStates.computeIfAbsent(forecastIndex, k -> new IndexState(k.getMapping())), conglomerateListeneer);
                    break;
                default:
                    // we don't have settings to update for other indices
                    IndexState indexState = indexStates.computeIfAbsent(forecastIndex, k -> new IndexState(k.getMapping()));
                    indexState.settingUpToDate = true;
                    logger.info(new ParameterizedMessage("Mark [{}]'s setting up-to-date", forecastIndex.getIndexName()));
                    conglomerateListeneer.onResponse(null);
                    break;
            }

        }
    }

    /**
     * Update mapping if schema version changes.
     */
    private void updateMappingIfNecessary(GroupedActionListener<Void> delegateListeneer) {
        if (allMappingUpdated) {
            delegateListeneer.onResponse(null);
            return;
        }

        List<ForecastIndex> updates = new ArrayList<>();
        for (ForecastIndex index : ForecastIndex.values()) {
            Boolean updated = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).mappingUpToDate;
            if (Boolean.FALSE.equals(updated)) {
                updates.add(index);
            }
        }
        if (updates.size() == 0) {
            allMappingUpdated = true;
            delegateListeneer.onResponse(null);
            return;
        }

        final GroupedActionListener<Void> conglomerateListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> delegateListeneer.onResponse(null), exception -> {
                delegateListeneer.onResponse(null);
                logger.error("Fail to update AD indices' mappings", exception);
            }),
            updates.size()
        );

        for (ForecastIndex forecastIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s mapping", forecastIndex.getIndexName()));
            shouldUpdateIndex(forecastIndex, ActionListener.wrap(shouldUpdate -> {
                if (shouldUpdate) {
                    adminClient
                        .indices()
                        .putMapping(
                            new PutMappingRequest().indices(forecastIndex.getIndexName()).source(forecastIndex.getMapping(), XContentType.JSON),
                            ActionListener.wrap(putMappingResponse -> {
                                if (putMappingResponse.isAcknowledged()) {
                                    logger.info(new ParameterizedMessage("Succeeded in updating [{}]'s mapping", forecastIndex.getIndexName()));
                                    markMappingUpdated(forecastIndex);
                                } else {
                                    logger.error(new ParameterizedMessage("Fail to update [{}]'s mapping", forecastIndex.getIndexName()));
                                }
                                conglomerateListeneer.onResponse(null);
                            }, exception -> {
                                logger
                                    .error(
                                        new ParameterizedMessage(
                                            "Fail to update [{}]'s mapping due to [{}]",
                                            forecastIndex.getIndexName(),
                                            exception.getMessage()
                                        )
                                    );
                                conglomerateListeneer.onFailure(exception);
                            })
                        );
                } else {
                    // index does not exist or the version is already up-to-date.
                    // When creating index, new mappings will be used.
                    // We don't need to update it.
                    logger.info(new ParameterizedMessage("We don't need to update [{}]'s mapping", forecastIndex.getIndexName()));
                    markMappingUpdated(forecastIndex);
                    conglomerateListeneer.onResponse(null);
                }
            }, exception -> {
                logger
                    .error(
                        new ParameterizedMessage("Fail to check whether we should update [{}]'s mapping", forecastIndex.getIndexName()),
                        exception
                    );
                conglomerateListeneer.onFailure(exception);
            }));

        }
    }

    private void markMappingUpdated(ForecastIndex adIndex) {
        IndexState indexState = indexStates.computeIfAbsent(adIndex, k -> new IndexState(k.getMapping()));
        if (Boolean.FALSE.equals(indexState.mappingUpToDate)) {
            indexState.mappingUpToDate = Boolean.TRUE;
            logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", adIndex.getIndexName()));
        }
    }

    private void shouldUpdateIndex(ForecastIndex index, ActionListener<Boolean> thenDo) {
        boolean exists = false;
        if (index.isAlias()) {
            exists = TimeSeriesIndices.doesAliasExists(clusterService, index.getIndexName());
        } else {
            exists = TimeSeriesIndices.doesIndexExists(clusterService, index.getIndexName());
        }
        if (false == exists) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }

        Integer newVersion = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).schemaVersion;
        if (index.isAlias()) {
            GetAliasesRequest getAliasRequest = new GetAliasesRequest()
                .aliases(index.getIndexName())
                .indicesOptions(IndicesOptions.lenientExpandOpenHidden());
            adminClient.indices().getAliases(getAliasRequest, ActionListener.wrap(getAliasResponse -> {
                String concreteIndex = null;
                for (ObjectObjectCursor<String, List<AliasMetadata>> entry : getAliasResponse.getAliases()) {
                    if (false == entry.value.isEmpty()) {
                        // we assume the alias map to one concrete index, thus we can return after finding one
                        concreteIndex = entry.key;
                        break;
                    }
                }
                if (concreteIndex == null) {
                    thenDo.onResponse(Boolean.FALSE);
                    return;
                }
                shouldUpdateConcreteIndex(concreteIndex, newVersion, thenDo);
            }, exception -> logger.error(new ParameterizedMessage("Fail to get [{}]'s alias", index.getIndexName()), exception)));
        } else {
            shouldUpdateConcreteIndex(index.getIndexName(), newVersion, thenDo);
        }
    }

    /**
     *
     * @param index Index metadata
     * @return The schema version of the given Index
     */
    @Override
    public int getSchemaVersion(TimeSeriesIndex index) {
        if (index instanceof ForecastIndex) {
            throw new IllegalArgumentException("Expect an ForecastIndex, but got " + index.getClass());
        }
        IndexState indexState = this.indexStates.computeIfAbsent((ForecastIndex)index, k -> new IndexState(k.getMapping()));
        return indexState.schemaVersion;
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    @Override
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        super.initConfigIndex(markMappingUpToDate(ForecastIndex.CONFIG, actionListener));
    }

    /**
     * Create config index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        super.initJobIndex(markMappingUpToDate(ForecastIndex.JOB, actionListener));
    }

    @Override
    protected IndexRequest createDummyIndexRequest(String resultIndex) throws IOException {
        ForecastResult dummyResult = ForecastResult.getDummyResult();
        return new IndexRequest(resultIndex)
            .id(DUMMY_FORECAST_RESULT_ID)
            .source(dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
    }

    @Override
    protected DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException {
        return new DeleteRequest(resultIndex).id(DUMMY_FORECAST_RESULT_ID);
    }

}
