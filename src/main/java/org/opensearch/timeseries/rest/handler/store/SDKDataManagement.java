/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

import static org.opensearch.timeseries.util.IndexUtils.parseResultFieldConfigs;
import static org.opensearch.timeseries.util.IndexUtils.validateMappingFields;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.AossDirectSigningClientFactory;
import org.opensearch.timeseries.client.AwsSigV4RequestHeaders;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.client.DataPlaneClientFactoryContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.client.UnsignedClientFactory;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexState;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.EndpointResolverFactoryLoader;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DataPlaneServiceUtils;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.IndexResourceLoader;
import org.opensearch.transport.client.Client;

/**
 * SDK-backed config store that only handles index existence lifecycle.
 * Instead of replacing the client calls everywhere with different clients
 * (e.g., s3 or remote metadata SDK), I can switch the whole IndexManagement
 * to SdkDataManagement, which is safer.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Client required by remote SDK library.")
public abstract class SDKDataManagement<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexableResultType extends IndexableResult>
    implements
        DataManagement<IndexType> {
    private static final Logger LOG = LogManager.getLogger(SDKDataManagement.class);
    private static final DateTimeFormatter RESULT_BACKING_INDEX_DATE_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy.MM.dd")
        .withZone(ZoneOffset.UTC);
    private static final TimeValue[] RESULT_MAPPING_VALIDATION_RETRY_DELAYS = new TimeValue[] {
        TimeValue.timeValueMillis(200),
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueSeconds(1)
    };

    private record ResultIndexContext(String indexName, String tenantId, String dataSourceId) {
    }

    private final String configIndexName;
    private final String stateIndexName;
    private final String checkpointIndexName;
    private final Client client;
    private final String threadPoolName;
    private final Settings settings;
    private final ConfigDocumentStore configDocumentStore;
    private final DataSourceEndpointResolver endpointResolver;
    private final DataPlaneClientFactory dataPlaneClientFactory;
    private final Setting<String> remoteMetadataServiceName;
    private final boolean aossDataPlane;
    private volatile DataPlaneClientFactory configStoreDataPlaneClientFactory;

    private final String resultMapping;
    private final String dummyResultId;
    private final String dummyResultBody;
    private Map<String, Object> resultFieldConfigs;
    private final EnumMap<IndexType, IndexState> indexStates;
    private final String customResultIndexPrefix;
    private final int maxUpdateRunningTimes;
    private final Map<String, AtomicBoolean> updateRunning;
    private final Map<String, AtomicInteger> updateRunningTimes;
    private volatile Semaphore updateRunningSemaphore;
    private final Map<String, Boolean> allMappingUpdated;

    public SDKDataManagement(
        String configIndexName,
        String stateIndexName,
        String checkpointIndexName,
        Client client,
        NamedXContentRegistry xContentRegistry,
        String threadPoolName,
        Setting<Boolean> multiTenancyEnabled,
        Settings settings,
        Setting<String> remoteMetadataEndpoint,
        Setting<String> remoteMetadataServiceName,
        ConfigDocumentStore configDocumentStore,
        String resultMapping,
        String dummyResultId,
        String dummyResultBody,
        Class<IndexType> indexTypeClass,
        String customResultIndexPrefix,
        int maxUpdateRunningTimes,
        ClusterService clusterService,
        Setting<Integer> maxConcurrentMappingUpdatesSetting
    ) {
        this(
            configIndexName,
            stateIndexName,
            checkpointIndexName,
            client,
            xContentRegistry,
            threadPoolName,
            multiTenancyEnabled,
            settings,
            remoteMetadataEndpoint,
            remoteMetadataServiceName,
            configDocumentStore,
            resultMapping,
            dummyResultId,
            dummyResultBody,
            indexTypeClass,
            customResultIndexPrefix,
            maxUpdateRunningTimes,
            clusterService,
            maxConcurrentMappingUpdatesSetting,
            null
        );
    }

    public SDKDataManagement(
        String configIndexName,
        String stateIndexName,
        String checkpointIndexName,
        Client client,
        NamedXContentRegistry xContentRegistry,
        String threadPoolName,
        Setting<Boolean> multiTenancyEnabled,
        Settings settings,
        Setting<String> remoteMetadataEndpoint,
        Setting<String> remoteMetadataServiceName,
        ConfigDocumentStore configDocumentStore,
        String resultMapping,
        String dummyResultId,
        String dummyResultBody,
        Class<IndexType> indexTypeClass,
        String customResultIndexPrefix,
        int maxUpdateRunningTimes,
        ClusterService clusterService,
        Setting<Integer> maxConcurrentMappingUpdatesSetting,
        DataPlaneClientFactory dataPlaneClientFactory
    ) {
        this.configIndexName = Objects.requireNonNull(configIndexName, "configIndexName must not be null");
        this.stateIndexName = Objects.requireNonNull(stateIndexName, "stateIndexName must not be null");
        this.checkpointIndexName = Objects.requireNonNull(checkpointIndexName, "checkpointIndexName must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.threadPoolName = threadPoolName;
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.configDocumentStore = Objects.requireNonNull(configDocumentStore, "configDocumentStore must not be null");
        Objects.requireNonNull(clusterService, "clusterService must not be null");
        Objects.requireNonNull(maxConcurrentMappingUpdatesSetting, "maxConcurrentMappingUpdatesSetting must not be null");
        this.remoteMetadataServiceName = Objects.requireNonNull(remoteMetadataServiceName, "remoteMetadataServiceName must not be null");
        this.endpointResolver = EndpointResolverFactoryLoader.loadDataSourceEndpointResolver(settings, getClass().getClassLoader());
        this.dataPlaneClientFactory = dataPlaneClientFactory == null ? new UnsignedClientFactory(endpointResolver) : dataPlaneClientFactory;
        this.aossDataPlane = DataPlaneServiceUtils.isAossDataPlane(settings);
        this.resultMapping = resultMapping;
        this.dummyResultId = dummyResultId;
        this.dummyResultBody = dummyResultBody;
        this.customResultIndexPrefix = customResultIndexPrefix;
        this.maxUpdateRunningTimes = maxUpdateRunningTimes;
        this.updateRunning = new ConcurrentHashMap<>();
        this.updateRunningTimes = new ConcurrentHashMap<>();
        this.updateRunningSemaphore = new Semaphore(maxConcurrentMappingUpdatesSetting.get(settings));
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(maxConcurrentMappingUpdatesSetting, it -> updateRunningSemaphore = new Semaphore(it));
        this.allMappingUpdated = new ConcurrentHashMap<>();
        this.indexStates = new EnumMap<>(indexTypeClass);
    }

    private IndexState newIndexStates(IndexType index) {
        boolean isResultIndex = index.isResultIndex();
        // if the index is a result index, we need to use the mapping from the local
        // mapping file
        // otherwise, mappingUpToDate and settingUpToDate are always true in
        // SDKDataManagement since
        // we require external CDK deployment to ensure that.
        return isResultIndex ? new IndexState(index.getMapping()) : new IndexState(index.getMapping(), true, true);
    }

    protected boolean shouldAddUnsignedPayloadHeaderForResultIndexWrites() {
        return false;
    }

    protected boolean shouldUseAliasBackedCustomResultIndex() {
        return aossDataPlane == false;
    }

    protected String getAossCustomResultIndexName(String customResultIndexAlias) {
        return customResultIndexAlias;
    }

    @Override
    // CDK should have created the config index
    public boolean doesConfigIndexExist() {
        return true;
    }

    @Override
    // CDK should have created the config index
    public void initConfigIndex(ActionListener<CreateIndexResponse> listener) {
        listener.onResponse(new CreateIndexResponse(true, true, configIndexName));
    }

    @Override
    // CDK should have created the state index
    public boolean doesStateIndexExist() {
        return true;
    }

    @Override
    // CDK should have created the state index
    public void initStateIndex(ActionListener<CreateIndexResponse> listener) {
        listener.onResponse(new CreateIndexResponse(true, true, stateIndexName));
    }

    @Override
    public boolean doesCheckpointIndexExist() {
        return true;
    }

    @Override
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> listener) {
        listener.onResponse(new CreateIndexResponse(true, true, checkpointIndexName));
    }

    @Override
    public <T> void validateCustomIndexForBackendJob(
        String resultIndexOrAlias,
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        validateCustomIndexForBackendJob(resultIndexOrAlias, configId, user, roles, function, listener, tenantId, null);
    }

    @Override
    public <T> void validateCustomIndexForBackendJob(
        String resultIndexOrAlias,
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        doesResultIndexOrAliasExists(resultIndexOrAlias, ActionListener.wrap(exists -> {
            if (exists == false) {
                initCustomResultIndexDirectly(resultIndexOrAlias, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        executeOnCustomIndex(resultIndexOrAlias, configId, function, listener, tenantId, dataSourceId);
                    } else {
                        String error = "Creating custom result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new TimeSeriesException(error));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        executeOnCustomIndex(resultIndexOrAlias, configId, function, listener, tenantId, dataSourceId);
                    } else {
                        listener.onFailure(exception);
                    }
                }), tenantId, dataSourceId);
                return;
            }
            validateResultIndexMapping(resultIndexOrAlias, ActionListener.wrap(validMapping -> {
                if (validMapping) {
                    executeOnCustomIndex(resultIndexOrAlias, configId, function, listener, tenantId, dataSourceId);
                } else {
                    listener.onFailure(new EndRunException("Result index mapping is not correct", true));
                }
            }, listener::onFailure), tenantId, dataSourceId);
        }, listener::onFailure), tenantId, dataSourceId);
    }

    private <T> void executeOnCustomIndex(
        String resultIndexOrAlias,
        String securityLogId,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        try {
            validateResultIndexAndExecute(resultIndexOrAlias, function, true, listener, tenantId, dataSourceId);
        } catch (Exception e) {
            LOG.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }

    @Override
    public boolean doesJobIndexExist() {
        return true;
    }

    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        actionListener.onResponse(new CreateIndexResponse(true, true, CommonName.JOB_INDEX));
    }

    @Override
    public void update(String tenantId) {
        String tenantKey = Objects.requireNonNullElse(tenantId, "");
        // no setting to update in multi-tenancy environment since job storage is in
        // eventbridge.
        if (Boolean.TRUE.equals(allMappingUpdated.get(tenantKey))) {
            return;
        }
        AtomicBoolean tenantUpdateRunning = updateRunning.computeIfAbsent(tenantKey, key -> new AtomicBoolean(false));
        AtomicInteger tenantUpdateRunningTimes = updateRunningTimes.computeIfAbsent(tenantKey, key -> new AtomicInteger(0));
        if (tenantUpdateRunningTimes.get() >= maxUpdateRunningTimes) {
            return;
        }
        // compareAndSet(false, true) makes the “check + set” atomic so only one thread
        // wins.
        if (!tenantUpdateRunning.compareAndSet(false, true)) {
            return;
        }
        Semaphore updateSemaphore = updateRunningSemaphore;
        if (!updateSemaphore.tryAcquire()) {
            tenantUpdateRunning.set(false);
            return;
        }
        tenantUpdateRunningTimes.incrementAndGet();

        // We only need to update custom result index related mapping
        // no default result index in multi-tenancy environment.
        try {
            updateCustomResultIndexMapping(tenantId, ActionListener.wrap(r -> {
                allMappingUpdated.put(tenantKey, true);
                tenantUpdateRunning.set(false);
                updateSemaphore.release();
            }, exception -> {
                tenantUpdateRunning.set(false);
                updateSemaphore.release();
                LOG.error("Fail to update time series indices", exception);
            }));
        } catch (Exception e) {
            tenantUpdateRunning.set(false);
            updateSemaphore.release();
            throw e;
        }
    }

    private void updateCustomResultIndexMapping(String tenantId, ActionListener<Void> listener) {
        getConfigsWithCustomResultIndexAlias(tenantId, ActionListener.wrap(candidateResultIndices -> {
            if (candidateResultIndices == null || candidateResultIndices.isEmpty()) {
                LOG.info("candidate custom result indices are empty.");
                listener.onResponse(null);
                return;
            }

            final GroupedActionListener<Void> customIndexMappingUpdateListener = new GroupedActionListener<>(
                ActionListener.wrap(mappingUpdateResponse -> {
                    listener.onResponse(null);
                }, exception -> {
                    listener.onResponse(null);
                    LOG.error("Fail to update result indices' mappings", exception);
                }),
                candidateResultIndices.size()
            );

            processResultIndexMappingIteration(0, candidateResultIndices, tenantId, customIndexMappingUpdateListener);
        }, e -> listener.onFailure(new TimeSeriesException("Fail to update custom result indices' mapping.", e))));
    }

    private void getConfigsWithCustomResultIndexAlias(String tenantId, ActionListener<List<ResultIndexContext>> listener) {
        // Assuming config index is accessible via client
        if (configIndexName == null) {
            listener.onResponse(new ArrayList<>());
            return;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.filter(QueryBuilders.wildcardQuery(Config.RESULT_INDEX_FIELD, customResultIndexPrefix + "*"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10000).query(boolQuery);

        SearchRequest searchRequest = new SearchRequest(configIndexName).source(sourceBuilder);
        TenantContext tenantContext = tenantId == null ? TenantContext.systemWide() : TenantContext.user(tenantId);
        configDocumentStore.search(searchRequest, tenantContext, ActionListener.wrap(searchResponse -> {
            if (searchResponse == null || searchResponse.getHits() == null) {
                LOG.info("no config available.");
                listener.onResponse(new ArrayList<>());
                return;
            }

            Iterator<SearchHit> iterator = searchResponse.getHits().iterator();

            Set<ResultIndexContext> candidateResultIndices = new LinkedHashSet<>();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                String resultIndex = (String) sourceAsMap.get(Config.RESULT_INDEX_FIELD);
                if (resultIndex != null && resultIndex.startsWith(customResultIndexPrefix)) {
                    String configTenantId = resolveConfigTenantId(sourceAsMap, tenantId);
                    String dataSourceId = (String) sourceAsMap.get(CommonName.DATA_SOURCE_ID_FIELD);
                    if (dataSourceId == null) {
                        LOG
                            .warn(
                                "Skipping custom result index [{}] for config [{}] because [{}] is missing",
                                resultIndex,
                                searchHit.getId(),
                                CommonName.DATA_SOURCE_ID_FIELD
                            );
                        continue;
                    }
                    candidateResultIndices.add(new ResultIndexContext(resultIndex, configTenantId, dataSourceId));
                }
            }
            listener.onResponse(new ArrayList<>(candidateResultIndices));
        }, e -> {
            if (e instanceof OpenSearchStatusException && ExceptionUtil.isIndexNotFoundInMessage(e)) {
                listener.onResponse(new ArrayList<>());
            } else {
                listener.onFailure(new TimeSeriesException("Fail to search configs via config store", e));
            }
        }));
    }

    private void processResultIndexMappingIteration(
        int indexPos,
        List<ResultIndexContext> candidateResultIndices,
        String tenantId,
        GroupedActionListener<Void> conglomerateListeneer
    ) {
        if (indexPos >= candidateResultIndices.size()) {
            return;
        }
        ResultIndexContext resultIndexContext = candidateResultIndices.get(indexPos);
        String index = resultIndexContext.indexName();
        String configTenantId = resultIndexContext.tenantId();
        String dataSourceId = resultIndexContext.dataSourceId();
        DataPlaneClientFactory.RequestContext requestContext = createConfigStoreRequestContext(resultIndexContext);

        LOG.info(new ParameterizedMessage("Check [{}]'s mapping", index));

        ActionListener<Boolean> validationListener = ActionListener.wrap(valid -> {
            if (!valid) {
                ActionListener<Boolean> updateListener = ActionListener.wrap(response -> {
                    conglomerateListeneer.onResponse(null);
                    processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
                }, e -> {
                    LOG.error("Fail to update mapping for " + index, e);
                    conglomerateListeneer.onFailure(e);
                    processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
                });
                DataPlaneClientFactoryContext
                    .runWithRequestContext(requestContext, () -> updateMapping(index, configTenantId, dataSourceId, updateListener));
            } else {
                conglomerateListeneer.onResponse(null);
                processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
            }
        }, e -> {
            LOG.error("Fail to validate mapping for " + index, e);
            conglomerateListeneer.onFailure(e);
            processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
        });
        DataPlaneClientFactoryContext
            .runWithRequestContext(
                requestContext,
                () -> validateResultIndexMapping(index, validationListener, configTenantId, dataSourceId)
            );
    }

    private String resolveConfigTenantId(Map<String, Object> sourceAsMap, String fallbackTenantId) {
        Object tenantId = sourceAsMap.get(CommonName.TENANT_ID_FIELD);
        return tenantId instanceof String ? (String) tenantId : fallbackTenantId;
    }

    /**
     * Custom-result mapping maintenance iterates config documents that may belong to different tenants.
     * Build the routing context from the persisted config metadata and the config-store/data-source resolver,
     * not from the inbound API ThreadContext.
     */
    private DataPlaneClientFactory.RequestContext createConfigStoreRequestContext(ResultIndexContext resultIndexContext) {
        return getConfigStoreDataPlaneClientFactory()
            .createRequestContext(resultIndexContext.tenantId(), resultIndexContext.dataSourceId());
    }

    private DataPlaneClientFactory getConfigStoreDataPlaneClientFactory() {
        DataPlaneClientFactory currentFactory = configStoreDataPlaneClientFactory;
        if (currentFactory == null) {
            synchronized (this) {
                currentFactory = configStoreDataPlaneClientFactory;
                if (currentFactory == null) {
                    currentFactory = createConfigStoreDataPlaneClientFactory();
                    configStoreDataPlaneClientFactory = currentFactory;
                }
            }
        }
        return currentFactory;
    }

    private DataPlaneClientFactory createConfigStoreDataPlaneClientFactory() {
        if (aossDataPlane) {
            String region = TimeSeriesSettings.REGION.get(settings);
            if (region == null || region.isBlank()) {
                throw new IllegalStateException(
                    TimeSeriesSettings.REGION.getKey() + " must be configured for AOSS config-store data-plane routing."
                );
            }
            return new AossDirectSigningClientFactory(
                region,
                endpointResolver,
                TimeSeriesSettings.BACKGROUND_JOB_ASSUME_ROLE_ARN.get(settings),
                remoteMetadataServiceName.get(settings),
                TimeSeriesSettings.DATA_PLANE_ENDPOINT_REGION_SIGNING_ENABLED.get(settings)
            );
        }
        return UnsignedClientFactory.background(endpointResolver);
    }

    private void updateMapping(String index, String tenantId, String dataSourceId, ActionListener<Boolean> listener) {
        try {
            String body = resultMapping;
            Response response = sendWithBody("PUT", "/" + index + "/_mapping", body, tenantId, dataSourceId);
            if (response != null && is2xx(response)) {
                listener.onResponse(true);
            } else {
                LOG
                    .error(
                        "Failed to update mapping for {}, status: {}",
                        index,
                        response == null ? "null" : response.getStatusLine().getStatusCode()
                    );
                listener.onFailure(new RuntimeException("Failed to update mapping"));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public boolean doesDefaultResultIndexExist() {
        // we don't have a default result index in multi-tenancy environment;
        // return true to avoid creating a default result index in places like
        // IndexMemoryPressureAwareResultHandler.flush.
        return true;
    }

    /**
     * AOSS cluster would hang with synchro HEAD _alias requests. Changed to use a
     * asynchronous GET _alias request instead.
     * 
     * @param indexName 
     *            the name of the index to check
     * @param listener
     *            the listener to receive the result
     * @param tenantId
     *            the tenant id
     */
    @Override
    public void doesResultIndexExists(String indexName, ActionListener<Boolean> listener, String tenantId) {
        doesResultIndexExists(indexName, listener, tenantId, null);
    }

    @Override
    public void doesResultIndexExists(String indexName, ActionListener<Boolean> listener, String tenantId, String dataSourceId) {
        existenceRequest("/" + indexName, tenantId, dataSourceId, listener);
    }

    @Override
    public void doesResultAliasExists(String aliasName, ActionListener<Boolean> listener, String tenantId) {
        doesResultAliasExists(aliasName, listener, tenantId, null);
    }

    @Override
    public void doesResultAliasExists(String aliasName, ActionListener<Boolean> listener, String tenantId, String dataSourceId) {
        existenceRequest("/_alias/" + aliasName, tenantId, dataSourceId, listener);
    }

    @Override
    public void doesResultIndexOrAliasExists(
        String indexOrAliasName,
        ActionListener<Boolean> listener,
        String tenantId,
        String dataSourceId
    ) {
        if (shouldUseAliasBackedCustomResultIndex() == false) {
            doesResultIndexExists(indexOrAliasName, listener, tenantId, dataSourceId);
            return;
        }
        doesResultIndexExists(indexOrAliasName, ActionListener.wrap(indexExists -> {
            if (indexExists) {
                listener.onResponse(true);
                return;
            }
            doesResultAliasExists(indexOrAliasName, listener, tenantId, dataSourceId);
        }, listener::onFailure), tenantId, dataSourceId);
    }

    @Override
    public int getSchemaVersion(IndexType index) {
        return indexStates.computeIfAbsent(index, k -> newIndexStates(k)).schemaVersion;
    }

    /**
     * Capture or reuse the request-scoped data-plane client context for this tenant and data source.
     */
    private DataPlaneClientFactory.RequestContext getRequestContext(String tenantId, String dataSourceId) {
        DataPlaneClientFactory.RequestContext currentContext = DataPlaneClientFactoryContext.getCurrentRequestContext();
        if (currentContext != null
            && Objects.equals(currentContext.tenantId(), tenantId)
            && Objects.equals(currentContext.dataSourceId(), dataSourceId)) {
            return currentContext;
        }

        try {
            return dataPlaneClientFactory.getOrCreateRequestContext(tenantId, dataSourceId);
        } catch (OpenSearchStatusException e) {
            if (hasDataSourceId(dataSourceId) && isMissingDataPlaneEndpoint(e)) {
                return getConfigStoreDataPlaneClientFactory().createRequestContext(tenantId, dataSourceId);
            }
            throw e;
        }
    }

    private boolean hasDataSourceId(String dataSourceId) {
        return dataSourceId != null && dataSourceId.isBlank() == false;
    }

    private boolean isMissingDataPlaneEndpoint(OpenSearchStatusException exception) {
        String message = exception.getMessage();
        return message != null && message.contains("Missing data plane endpoint in ThreadContext key");
    }

    private void performRequestAsync(DataPlaneClientFactory.RequestContext requestContext, Request request, ResponseListener listener) {
        Releasable dataPlaneRequestContext = requestContext.prepareRequest(request);
        boolean submitted = false;
        try {
            requestContext.restClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> listener.onSuccess(response));
                    } finally {
                        releaseRequestContext(dataPlaneRequestContext);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> listener.onFailure(e));
                    } finally {
                        releaseRequestContext(dataPlaneRequestContext);
                    }
                }
            });
            submitted = true;
        } finally {
            if (submitted == false) {
                releaseRequestContext(dataPlaneRequestContext);
            }
        }
    }

    private Response performRequest(DataPlaneClientFactory.RequestContext requestContext, Request request) throws Exception {
        Releasable dataPlaneRequestContext = requestContext.prepareRequest(request);
        try {
            return requestContext.restClient().performRequest(request);
        } finally {
            releaseRequestContext(dataPlaneRequestContext);
        }
    }

    private void releaseRequestContext(Releasable requestContext) {
        try {
            requestContext.close();
        } catch (Exception e) {
            LOG.warn("Failed to release REST data-plane request context", e);
        }
    }

    private boolean is2xx(Response response) {
        int status = response.getStatusLine().getStatusCode();
        return status >= 200 && status < 300;
    }

    private String getResponseBody(Response response) {
        try {
            return response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            LOG.warn("Failed to read response body", e);
            return null;
        }
    }

    private String formatResponseDetails(Response response) {
        return formatResponseDetails(response, getResponseBody(response));
    }

    private String formatResponseDetails(Response response, String body) {
        if (response == null) {
            return "null response";
        }
        String status = response.getStatusLine() == null ? "null" : Integer.toString(response.getStatusLine().getStatusCode());
        return "status=" + status + ", body=" + body;
    }

    private void existenceRequest(String path, String tenantId, String dataSourceId, ActionListener<Boolean> listener) {
        DataPlaneClientFactory.RequestContext requestContext = getRequestContext(tenantId, dataSourceId);
        try {
            // AOSS handles the GET forms reliably; in live tests the equivalent HEAD
            // request can hang until the caller's REST timeout expires.
            Request request = new Request(DataPlaneServiceUtils.isAossDataPlane(settings) ? "GET" : "HEAD", path);
            performRequestAsync(requestContext, request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    boolean exists = is2xx(response);
                    consumeResponse(response);
                    listener.onResponse(exists);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResponseException) {
                        Response response = ((ResponseException) e).getResponse();
                        consumeResponse(response);
                        listener
                            .onResponse(
                                response != null
                                    && response.getStatusLine() != null
                                    && response.getStatusLine().getStatusCode() != 404
                                    && is2xx(response)
                            );
                        return;
                    }
                    LOG.warn("Failed to check existence for path {}", path, e);
                    listener.onResponse(false);
                }
            });
        } catch (Exception e) {
            LOG.warn("Failed to check existence for path {}", path, e);
            DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> listener.onResponse(false));
        }
    }

    private boolean isAossCustomResultAliasName(String indexName) {
        return DataPlaneServiceUtils.isAossDataPlane(settings)
            && indexName != null
            && indexName.startsWith(customResultIndexPrefix)
            && indexName.contains("-history-") == false;
    }

    private void consumeResponse(Response response) {
        if (response == null || response.getEntity() == null) {
            return;
        }
        try {
            EntityUtils.consume(response.getEntity());
        } catch (Exception e) {
            LOG.debug("Failed to consume response entity", e);
        }
    }

    private Response sendWithBody(String method, String path, String body, String tenantId, String dataSourceId) {
        DataPlaneClientFactory.RequestContext requestContext = getRequestContext(tenantId, dataSourceId);
        try {
            Request request = new Request(method, path);
            if (body != null) {
                request.setJsonEntity(body);
            }
            addUnsignedPayloadHeaderForResultIndexWrite(request, requestContext);
            return performRequest(requestContext, request);
        } catch (ResponseException e) {
            return e.getResponse();
        } catch (Exception e) {
            LOG.warn("Failed to {} {}", method, path, e);
            return null;
        }
    }

    private void addUnsignedPayloadHeaderForResultIndexWrite(Request request, DataPlaneClientFactory.RequestContext requestContext) {
        if (shouldAddUnsignedPayloadHeaderForResultIndexWrites() && requestContext != null && requestContext.tenantId() != null) {
            AwsSigV4RequestHeaders.addUnsignedPayloadHeader(request);
        }
    }

    @Override
    public <T> void validateResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        boolean mappingValidated,
        ActionListener<T> listener,
        String tenantId
    ) {
        validateResultIndexAndExecute(resultIndexOrAlias, function, mappingValidated, listener, tenantId, null);
    }

    @Override
    public <T> void validateResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        boolean mappingValidated,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        if (!mappingValidated) {
            validateResultIndexMapping(resultIndexOrAlias, ActionListener.wrap(validMapping -> {
                if (validMapping) {
                    executeAfterValidateResultIndexMapping(resultIndexOrAlias, function, listener, tenantId, dataSourceId);
                } else {
                    LOG.warn("Can't create analysis with custom result index {} as its mapping is invalid", resultIndexOrAlias);
                    listener.onFailure(new IllegalArgumentException(CommonMessages.INVALID_RESULT_INDEX_MAPPING + resultIndexOrAlias));
                }
            }, listener::onFailure), tenantId, dataSourceId);
        } else {
            try {
                executeAfterValidateResultIndexMapping(resultIndexOrAlias, function, listener, tenantId, dataSourceId);
            } catch (Exception e) {
                LOG.error("Failed to validate custom result index " + resultIndexOrAlias, e);
                listener.onFailure(e);
            }
        }
    }

    private void initResultMapping() {
        if (resultFieldConfigs != null) {
            return;
        }
        resultFieldConfigs = parseResultFieldConfigs(resultMapping, LOG);
    }

    @Override
    public void validateResultIndexMapping(
        String resultIndexOrAlias,
        ActionListener<Boolean> thenDo,
        String tenantId,
        String dataSourceId
    ) {
        getConcreteIndex(resultIndexOrAlias, ActionListener.wrap(concreteIndex -> {
            try {
                initResultMapping();
                if (resultFieldConfigs == null) {
                    thenDo.onResponse(false);
                    return;
                }

                DataPlaneClientFactory.RequestContext requestContext = getRequestContext(tenantId, dataSourceId);
                getResultIndexMappingWithRetry(requestContext, concreteIndex, thenDo, 1);

            } catch (Exception e) {
                LOG.error("Failed to validate result index mapping for index " + concreteIndex, e);
                thenDo.onResponse(false);
            }
        }, thenDo::onFailure), tenantId, dataSourceId);
    }

    private void getResultIndexMappingWithRetry(
        DataPlaneClientFactory.RequestContext requestContext,
        String concreteIndex,
        ActionListener<Boolean> thenDo,
        int attempt
    ) {
        Request mappingRequest = new Request("GET", "/" + concreteIndex + "/_mapping");
        performRequestAsync(requestContext, mappingRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                if (is2xx(response)) {
                    validateResultIndexMappingResponse(response, concreteIndex, thenDo);
                    return;
                }

                int status = response.getStatusLine().getStatusCode();
                if (shouldRetryResultMappingValidation(status, attempt)) {
                    consumeResponse(response);
                    retryResultIndexMapping(requestContext, concreteIndex, thenDo, attempt, "status " + status, null);
                    return;
                }

                LOG.warn("Failed to get mapping for index {}. Status: {}", concreteIndex, status);
                consumeResponse(response);
                thenDo.onResponse(false);
            }

            @Override
            public void onFailure(Exception e) {
                Response failureResponse = response(e);
                Integer status = responseStatus(failureResponse);
                if (status != null && shouldRetryResultMappingValidation(status, attempt)) {
                    consumeResponse(failureResponse);
                    retryResultIndexMapping(requestContext, concreteIndex, thenDo, attempt, "status " + status, e);
                    return;
                }

                LOG.error("Failed to get mapping for index " + concreteIndex, e);
                thenDo.onFailure(new RuntimeException(e));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void validateResultIndexMappingResponse(Response response, String concreteIndex, ActionListener<Boolean> thenDo) {
        try {
            String body = getResponseBody(response);
            Map<String, Object> responseMap = XContentHelper.convertToMap(new BytesArray(body), false, XContentType.JSON).v2();
            // Response format: { "index_name": { "mappings": { "properties": { ... } } } }
            Map<String, Object> indexRoot = (Map<String, Object>) responseMap.get(concreteIndex);
            if (indexRoot == null) {
                thenDo.onResponse(false);
                return;
            }
            Map<String, Object> mappings = (Map<String, Object>) indexRoot.get("mappings");
            if (mappings == null) {
                thenDo.onResponse(false);
                return;
            }

            Object propertiesObj = mappings.get(CommonName.PROPERTIES);
            if (!(propertiesObj instanceof Map)) {
                thenDo.onResponse(false);
                return;
            }

            Map<String, Object> actualMapping = (Map<String, Object>) propertiesObj;
            thenDo.onResponse(validateMappingFields(actualMapping, resultFieldConfigs, LOG));
        } catch (Exception e) {
            LOG.error("Failed to parse mapping response for index " + concreteIndex, e);
            thenDo.onResponse(false);
        }
    }

    private void retryResultIndexMapping(
        DataPlaneClientFactory.RequestContext requestContext,
        String concreteIndex,
        ActionListener<Boolean> thenDo,
        int attempt,
        String reason,
        Exception exception
    ) {
        TimeValue delay = RESULT_MAPPING_VALIDATION_RETRY_DELAYS[attempt - 1];
        LOG
            .warn(
                "Transient failure getting mapping for index {} on attempt {}/{}. Retrying after {} because {}",
                concreteIndex,
                attempt,
                RESULT_MAPPING_VALIDATION_RETRY_DELAYS.length + 1,
                delay,
                reason,
                exception
            );

        ThreadPool threadPool = client.threadPool();
        if (threadPool == null) {
            getResultIndexMappingWithRetry(requestContext, concreteIndex, thenDo, attempt + 1);
            return;
        }
        threadPool.schedule(
            () -> getResultIndexMappingWithRetry(requestContext, concreteIndex, thenDo, attempt + 1),
            delay,
            retryExecutorName()
        );
    }

    private String retryExecutorName() {
        return threadPoolName == null || threadPoolName.isBlank() ? ThreadPool.Names.GENERIC : threadPoolName;
    }

    private boolean shouldRetryResultMappingValidation(int status, int attempt) {
        return attempt <= RESULT_MAPPING_VALIDATION_RETRY_DELAYS.length && isTransientResultMappingStatus(status);
    }

    private boolean isTransientResultMappingStatus(int status) {
        return status == 403 || status == 404 || status == 429 || status >= 500;
    }

    private Response response(Exception e) {
        return e instanceof ResponseException responseException ? responseException.getResponse() : null;
    }

    private Integer responseStatus(Response response) {
        if (response != null && response.getStatusLine() != null) {
            return response.getStatusLine().getStatusCode();
        }
        return null;
    }

    private void getConcreteIndex(String indexOrAliasName, ActionListener<String> thenDo, String tenantId, String dataSourceId) {
        if (shouldUseAliasBackedCustomResultIndex() == false) {
            thenDo.onResponse(indexOrAliasName);
            return;
        }

        // First check if alias exists before attempting to resolve
        doesResultAliasExists(indexOrAliasName, ActionListener.wrap(aliasExists -> {
            if (aliasExists) {
                Request request = new Request("GET", "/_alias/" + indexOrAliasName);
                DataPlaneClientFactory.RequestContext requestContext = getRequestContext(tenantId, dataSourceId);

                performRequestAsync(requestContext, request, new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        if (response.getStatusLine().getStatusCode() == 200) {
                            try {
                                String body = getResponseBody(response);
                                Map<String, Object> responseMap = XContentHelper
                                    .convertToMap(new BytesArray(body), false, XContentType.JSON)
                                    .v2();
                                if (!responseMap.isEmpty()) {
                                    // Get first key as concrete index (we assume alias maps to one concrete index)
                                    String concreteIndex = responseMap.keySet().iterator().next();
                                    thenDo.onResponse(concreteIndex);
                                } else {
                                    thenDo.onResponse(indexOrAliasName);
                                }
                            } catch (Exception e) {
                                LOG.error("Failed to parse alias response", e);
                                thenDo.onResponse(indexOrAliasName);
                            }
                        } else {
                            // Unexpected response, return original name
                            thenDo.onResponse(indexOrAliasName);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOG.error("Failed to resolve alias " + indexOrAliasName, e);
                        thenDo.onResponse(indexOrAliasName);
                    }
                });
            } else {
                // If this is not an alias or the index does not exist yet, return
                // indexOrAliasName
                thenDo.onResponse(indexOrAliasName);
            }
        }, thenDo::onFailure), tenantId, dataSourceId);
    }

    private <T> void executeAfterValidateResultIndexMapping(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        if (aossDataPlane) {
            /*
             * AOSS supports PUT /<index>/_doc/<id> only for SEARCH collection types.
             * AD result writes use bulk indexing, so the AOSS path skips the dummy
             * single-doc write/delete probe and lets the real bulk write surface
             * permission or data-policy failures.
             */
            executeValidatedFunction(function, listener);
            return;
        }

        DataPlaneClientFactory.RequestContext requestContext = getRequestContext(tenantId, dataSourceId);

        // Write dummy doc with a unique id to avoid concurrent delete collisions.
        final String requestDummyId = dummyResultId + "-" + UUID.randomUUID();
        Request writeRequest = new Request("PUT", "/" + resultIndexOrAlias + "/_doc/" + requestDummyId);
        writeRequest.setJsonEntity(dummyResultBody);
        addUnsignedPayloadHeaderForResultIndexWrite(writeRequest, requestContext);

        performRequestAsync(requestContext, writeRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response writeResponse) {
                String writeResponseBody = getResponseBody(writeResponse);
                String writeResponseDetails = formatResponseDetails(writeResponse, writeResponseBody);
                if (is2xx(writeResponse)) {
                    LOG.debug("Successfully wrote dummy result to result index {}", resultIndexOrAlias);
                    deleteDummyResult(
                        requestContext,
                        resultIndexOrAlias,
                        requestDummyId,
                        parseIndexedConcreteIndex(writeResponseBody),
                        writeResponseDetails,
                        function,
                        listener
                    );
                } else {
                    String error = "Failed to write dummy result to result index "
                        + resultIndexOrAlias
                        + ". Status: "
                        + writeResponse.getStatusLine().getStatusCode();
                    LOG.error("{}; response: {}", error, writeResponseDetails);
                    listener.onFailure(new RuntimeException(error));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Failed to write dummy result to result index " + resultIndexOrAlias, e);
                listener.onFailure(new RuntimeException(e));
            }
        });
    }

    private <T> void deleteDummyResult(
        DataPlaneClientFactory.RequestContext requestContext,
        String deleteIndexOrAlias,
        String requestDummyId,
        String fallbackConcreteIndex,
        String writeResponseDetails,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        Request deleteRequest = new Request("DELETE", "/" + deleteIndexOrAlias + "/_doc/" + requestDummyId);

        performRequestAsync(requestContext, deleteRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response deleteResponse) {
                if (is2xx(deleteResponse)) {
                    LOG.info("Successfully deleted dummy result from result index {}", deleteIndexOrAlias);
                    executeValidatedFunction(function, listener);
                } else if (shouldRetryDeleteOnConcreteIndex(deleteResponse, deleteIndexOrAlias, fallbackConcreteIndex)) {
                    // AOSS-only fallback: write succeeded via the alias but the by-id DELETE through the
                    // same alias returned 404 / index_not_found_exception. See
                    // retryDeleteDummyResultOnConcreteIndex(...) for the full rationale.
                    retryDeleteDummyResultOnConcreteIndex(
                        requestContext,
                        deleteIndexOrAlias,
                        requestDummyId,
                        fallbackConcreteIndex,
                        writeResponseDetails,
                        function,
                        listener
                    );
                } else {
                    String error = "Failed to delete dummy result from result index "
                        + deleteIndexOrAlias
                        + ". Status: "
                        + deleteResponse.getStatusLine().getStatusCode();
                    LOG.error(error);
                    listener.onFailure(new RuntimeException(error));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (shouldRetryDeleteOnConcreteIndex(e, deleteIndexOrAlias, fallbackConcreteIndex)) {
                    retryDeleteDummyResultOnConcreteIndex(
                        requestContext,
                        deleteIndexOrAlias,
                        requestDummyId,
                        fallbackConcreteIndex,
                        writeResponseDetails,
                        function,
                        listener
                    );
                } else {
                    LOG
                        .error(
                            "Failed to delete dummy result from result index {}. Write response: {}",
                            deleteIndexOrAlias,
                            writeResponseDetails,
                            e
                        );
                    listener.onFailure(new RuntimeException(e));
                }
            }
        });
    }

    /**
     * AOSS-specific cleanup fallback for the validation dummy-write/dummy-delete flow.
     *
     * <p>On a normal OpenSearch cluster, {@code PUT /<alias>/_doc/<id>} and {@code DELETE /<alias>/_doc/<id>}
     * resolve the alias the same way and both succeed. On OpenSearch Serverless (AOSS) we have observed
     * the write succeeding (returning a concrete {@code _index} for the doc) while the by-id delete
     * through the same alias comes back with {@code 404 index_not_found_exception} - i.e. the alias
     * itself was reported as nonexistent, not the doc. Plausible AOSS-side causes:
     * <ul>
     *   <li>Asymmetric alias resolution between write and single-doc delete handlers.</li>
     *   <li>Eventual consistency of alias-to-backing-index mapping across data-plane nodes.</li>
     *   <li>Multi-backing-index aliases (rollovers) that AOSS's by-id DELETE refuses to fan out to.</li>
     * </ul>
     *
     * <p>The original write response carries the actual concrete index the engine wrote to (parsed via
     * {@link #parseIndexedConcreteIndex(String)}), which is authoritative and bypasses any further alias
     * resolution. We retry the cleanup directly against that concrete index so the dummy probe doc is
     * not leaked. The retry passes {@code null} as the fallback to prevent another retry layer.
     */
    private <T> void retryDeleteDummyResultOnConcreteIndex(
        DataPlaneClientFactory.RequestContext requestContext,
        String failedIndexOrAlias,
        String requestDummyId,
        String fallbackConcreteIndex,
        String writeResponseDetails,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        LOG
            .warn(
                "Failed to delete dummy result through [{}] because the index or alias was not found. Retrying concrete index [{}]. Write response: {}",
                failedIndexOrAlias,
                fallbackConcreteIndex,
                writeResponseDetails
            );
        deleteDummyResult(requestContext, fallbackConcreteIndex, requestDummyId, null, writeResponseDetails, function, listener);
    }

    private <T> void executeValidatedFunction(ExecutorFunction function, ActionListener<T> listener) {
        try {
            function.execute();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String parseIndexedConcreteIndex(String responseBody) {
        if (responseBody == null) {
            return null;
        }
        try {
            Map<String, Object> responseMap = XContentHelper.convertToMap(new BytesArray(responseBody), false, XContentType.JSON).v2();
            Object index = responseMap.get("_index");
            return index instanceof String ? (String) index : null;
        } catch (Exception e) {
            LOG.warn("Failed to parse dummy result write response body", e);
            return null;
        }
    }

    /**
     * Narrow gate for the AOSS dummy-delete fallback (see {@link #retryDeleteDummyResultOnConcreteIndex}).
     * Returns true only when:
     * <ul>
     *   <li>the original write response gave us a real concrete {@code _index} ({@code fallbackConcreteIndex != null}),</li>
     *   <li>the failing DELETE was against an alias rather than that concrete index (so we won't loop on a real bug), and</li>
     *   <li>the response is strictly {@code 404 index_not_found_exception} (i.e. alias resolution failed,
     *       not "doc not found", which would already be a benign outcome for the probe doc).</li>
     * </ul>
     */
    private boolean shouldRetryDeleteOnConcreteIndex(Response response, String deleteIndexOrAlias, String fallbackConcreteIndex) {
        return fallbackConcreteIndex != null && !fallbackConcreteIndex.equals(deleteIndexOrAlias) && isIndexNotFoundResponse(response);
    }

    private boolean shouldRetryDeleteOnConcreteIndex(Exception e, String deleteIndexOrAlias, String fallbackConcreteIndex) {
        if (!(e instanceof ResponseException)) {
            return false;
        }
        return shouldRetryDeleteOnConcreteIndex(((ResponseException) e).getResponse(), deleteIndexOrAlias, fallbackConcreteIndex);
    }

    private boolean isIndexNotFoundResponse(Response response) {
        if (response == null || response.getStatusLine() == null || response.getStatusLine().getStatusCode() != 404) {
            return false;
        }
        String body = getResponseBody(response);
        return body != null && body.contains("index_not_found_exception");
    }

    private boolean isResourceAlreadyExists(Response response) {
        if (response == null) {
            return false;
        }
        int status = response.getStatusLine().getStatusCode();
        if (status == 409) {
            return true;
        }
        if (status == 400) {
            String body = getResponseBody(response);
            return body != null && body.toLowerCase(Locale.ROOT).contains("resource_already_exists_exception");
        }
        return false;
    }

    private boolean isResourceAlreadyExists(Response response, String body) {
        if (response == null) {
            return false;
        }
        int status = response.getStatusLine().getStatusCode();
        if (status == 409) {
            return true;
        }
        if (status == 400) {
            return body != null && body.toLowerCase(Locale.ROOT).contains("resource_already_exists_exception");
        }
        return false;
    }

    private boolean isInvalidIndexName(Response response, String body) {
        if (response == null) {
            return false;
        }
        int status = response.getStatusLine().getStatusCode();
        if (status == 400) {
            return body != null && body.toLowerCase(Locale.ROOT).contains("invalid_index_name_exception");
        }
        return false;
    }

    @Override
    public void initCustomResultIndexDirectly(
        String resultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId,
        String dataSourceId
    ) {
        try {
            String originalPattern = TimeSeriesIndex.getCustomResultIndexPattern(resultIndexAlias);
            String createIndexName = aossDataPlane
                ? getAossCustomResultIndexName(resultIndexAlias)
                : encodeDateMathIndexName(originalPattern);
            String responseIndexName = aossDataPlane ? createIndexName : originalPattern;
            String bodyEntity = buildCustomResultIndexBody(resultIndexAlias);
            Response response = sendWithBody("PUT", "/" + createIndexName, bodyEntity, tenantId, dataSourceId);

            String responseBody = null;
            if (response != null && response.getStatusLine().getStatusCode() == 400) {
                responseBody = getResponseBody(response);
            }

            if (!aossDataPlane && isInvalidIndexName(response, responseBody)) {
                LOG.info("AOSS rejected Date Math index pattern. Falling back to explicit timestamp for {}...", resultIndexAlias);
                String fallbackIndexName = getConcreteCustomResultIndexName(resultIndexAlias);
                response = sendWithBody("PUT", "/" + fallbackIndexName, bodyEntity, tenantId, dataSourceId);

                String fallbackBody = null;
                if (response != null && response.getStatusLine().getStatusCode() == 400) {
                    fallbackBody = getResponseBody(response);
                }

                if (response != null && is2xx(response)) {
                    actionListener.onResponse(new CreateIndexResponse(true, true, fallbackIndexName));
                    return;
                } else if (isResourceAlreadyExists(response, fallbackBody)) {
                    actionListener.onFailure(new ResourceAlreadyExistsException(fallbackIndexName));
                    return;
                } else {
                    String error = "Creating fallback result index "
                        + fallbackIndexName
                        + " with alias "
                        + resultIndexAlias
                        + " failed with status "
                        + (response == null ? "null" : response.getStatusLine().getStatusCode());
                    LOG.error(error);
                    actionListener.onFailure(new RuntimeException(error));
                    return;
                }
            }

            if (response != null && is2xx(response)) {
                actionListener.onResponse(new CreateIndexResponse(true, true, responseIndexName));
            } else if (isResourceAlreadyExists(response, responseBody)) {
                actionListener.onFailure(new ResourceAlreadyExistsException(responseIndexName));
            } else {
                String error = "Creating result index "
                    + responseIndexName
                    + " with alias "
                    + resultIndexAlias
                    + " failed with status "
                    + (response == null ? "null" : response.getStatusLine().getStatusCode());
                LOG.error(error);
                actionListener.onFailure(new RuntimeException(error));
            }
        } catch (Exception e) {
            LOG.error("Failed to create result index for alias {}", resultIndexAlias, e);
            actionListener.onFailure(e);
        }
    }

    @Override
    public <T> void initCustomResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        initCustomResultIndexAndExecute(resultIndexOrAlias, function, listener, tenantId, null);
    }

    @Override
    public <T> void initCustomResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        doesResultIndexOrAliasExists(resultIndexOrAlias, ActionListener.wrap(exists -> {
            if (exists) {
                validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId, dataSourceId);
                return;
            }
            initCustomResultIndexDirectly(resultIndexOrAlias, ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    LOG.info("Successfully created result index {}", resultIndexOrAlias);
                    validateResultIndexAndExecute(resultIndexOrAlias, function, true, listener, tenantId, dataSourceId);
                } else {
                    // Creation may fail if another node created the index concurrently; treat as
                    // already exists.
                    validateExistingResultIndexAfterCreateAttempt(resultIndexOrAlias, function, listener, tenantId, dataSourceId, null);
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId, dataSourceId);
                } else {
                    validateExistingResultIndexAfterCreateAttempt(
                        resultIndexOrAlias,
                        function,
                        listener,
                        tenantId,
                        dataSourceId,
                        exception
                    );
                }
            }), tenantId, dataSourceId);
        }, listener::onFailure), tenantId, dataSourceId);
    }

    private <T> void validateExistingResultIndexAfterCreateAttempt(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId,
        Exception createFailure
    ) {
        doesResultIndexOrAliasExists(resultIndexOrAlias, ActionListener.wrap(exists -> {
            if (exists) {
                LOG.info("Result index {} already exists after create attempt, validating mapping", resultIndexOrAlias);
                validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId, dataSourceId);
                return;
            }
            if (createFailure == null) {
                String error = "Creating result index with mappings call not acknowledged: " + resultIndexOrAlias;
                LOG.error(error);
                listener.onFailure(new EndRunException(error, false));
            } else {
                LOG.error("Failed to create result index " + resultIndexOrAlias, createFailure);
                listener.onFailure(createFailure);
            }
        }, listener::onFailure), tenantId, dataSourceId);
    }

    @Override
    public void initFlattenedResultIndex(
        String flattenedResultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId
    ) {
        initFlattenedResultIndex(flattenedResultIndexAlias, actionListener, tenantId, null);
    }

    @Override
    public void initFlattenedResultIndex(
        String flattenedResultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId,
        String dataSourceId
    ) {
        try {
            String originalPattern = TimeSeriesIndex.getCustomResultIndexPattern(flattenedResultIndexAlias);
            String createIndexName = aossDataPlane
                ? getConcreteCustomResultIndexName(flattenedResultIndexAlias)
                : encodeDateMathIndexName(originalPattern);
            String responseIndexName = aossDataPlane ? createIndexName : originalPattern;
            StringBuilder bodyBuilder = new StringBuilder();
            bodyBuilder.append("{\"mappings\":").append(IndexResourceLoader.getFlattenedResultMappingsFromContent(resultMapping));
            if (flattenedResultIndexAlias != null) {
                bodyBuilder.append(",\"aliases\":{").append("\"").append(flattenedResultIndexAlias).append("\":{}}");
            }
            bodyBuilder.append("}");

            String bodyEntity = bodyBuilder.toString();
            Response response = sendWithBody("PUT", "/" + createIndexName, bodyEntity, tenantId, dataSourceId);

            String responseBody = null;
            if (response != null && response.getStatusLine().getStatusCode() == 400) {
                responseBody = getResponseBody(response);
            }

            if (!aossDataPlane && isInvalidIndexName(response, responseBody)) {
                LOG.info("AOSS rejected Date Math index pattern. Falling back to explicit timestamp for {}...", flattenedResultIndexAlias);
                String fallbackIndexName = getConcreteCustomResultIndexName(flattenedResultIndexAlias);
                response = sendWithBody("PUT", "/" + fallbackIndexName, bodyEntity, tenantId, dataSourceId);

                String fallbackBody = null;
                if (response != null && response.getStatusLine().getStatusCode() == 400) {
                    fallbackBody = getResponseBody(response);
                }

                if (response != null && is2xx(response)) {
                    actionListener.onResponse(new CreateIndexResponse(true, true, fallbackIndexName));
                    return;
                } else if (isResourceAlreadyExists(response, fallbackBody)) {
                    actionListener.onFailure(new ResourceAlreadyExistsException(fallbackIndexName));
                    return;
                } else {
                    String errorMsg = "Index creation not acknowledged for index: " + fallbackIndexName;
                    LOG.error(errorMsg);
                    actionListener.onFailure(new IllegalStateException(errorMsg));
                    return;
                }
            }

            if (response != null && is2xx(response)) {
                actionListener.onResponse(new CreateIndexResponse(true, true, responseIndexName));
            } else if (isResourceAlreadyExists(response, responseBody)) {
                actionListener.onFailure(new ResourceAlreadyExistsException(responseIndexName));
            } else {
                String errorMsg = "Index creation not acknowledged for index: " + responseIndexName;
                LOG.error(errorMsg);
                actionListener.onFailure(new IllegalStateException(errorMsg));
            }
        } catch (Exception e) {
            LOG.error("Error while initializing flattened result index: {}", flattenedResultIndexAlias, e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Encode date math index names (e.g., &lt;alias-{now/d}-1&gt;) so the REST
     * client URI builder accepts them.
     *
     * @param indexName the index name to encode
     * @return the encoded index name
     */
    private String encodeDateMathIndexName(String indexName) {
        return URLEncoder.encode(indexName, StandardCharsets.UTF_8);
    }

    private String buildCustomResultIndexBody(String resultIndexAlias) {
        StringBuilder bodyBuilder = new StringBuilder();
        bodyBuilder.append("{\"mappings\":").append(resultMapping);
        if (shouldUseAliasBackedCustomResultIndex()) {
            bodyBuilder.append(",\"aliases\":{\"").append(resultIndexAlias).append("\":{}}");
        }
        bodyBuilder.append("}");
        return bodyBuilder.toString();
    }

    /**
     * Resolve {now/d} client-side: AOSS Serverless rejects date-math index names
     * (e.g. {@code <alias-history-{now/d}-1>}) with invalid_index_name_exception.
     * Producing a fully-qualified name here matches what server-side {now/d} would
     * yield on self-managed OpenSearch and keeps rollover suffix conventions intact.
     *
     * @param customResultIndexAlias the custom result index alias
     * @return the concrete custom result index name
     */
    private String getConcreteCustomResultIndexName(String customResultIndexAlias) {
        return customResultIndexAlias + "-history-" + RESULT_BACKING_INDEX_DATE_FORMATTER.format(Instant.now()) + "-1";
    }

    @Override
    public void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) {
        // no default result index in multi-tenancy environment
        throw new UnsupportedOperationException("Unimplemented method 'initDefaultResultIndexDirectly'");
    }

    @Override
    public <T> void validateDefaultResultIndexForBackendJob(
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        throw new UnsupportedOperationException("validateDefaultResultIndexForBackendJob is not supported in SDKDataManagement");
    }
}
