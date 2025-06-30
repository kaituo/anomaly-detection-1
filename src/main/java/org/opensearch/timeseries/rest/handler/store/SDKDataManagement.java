/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

import static org.opensearch.timeseries.util.IndexUtils.parseResultFieldConfigs;
import static org.opensearch.timeseries.util.IndexUtils.validateMappingFields;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.client.SecurityHeaderInjector;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexState;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.spi.DefaultTenantEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.IndexResourceLoader;
import org.opensearch.timeseries.util.SdkClientProvider;
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

    private final SdkClient sdkClient;
    private final String configIndexName;
    private final String stateIndexName;
    private final String checkpointIndexName;
    private final Settings settings;
    private final TenantEndpointResolver endpointResolver;
    private final ThreadLocal<Map<String, String>> securityHeaders;

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
        String resultMapping,
        String dummyResultId,
        String dummyResultBody,
        Class<IndexType> indexTypeClass,
        String customResultIndexPrefix,
        int maxUpdateRunningTimes,
        ClusterService clusterService,
        Setting<Integer> maxConcurrentMappingUpdatesSetting
    ) {
        this.configIndexName = Objects.requireNonNull(configIndexName, "configIndexName must not be null");
        this.stateIndexName = Objects.requireNonNull(stateIndexName, "stateIndexName must not be null");
        this.checkpointIndexName = Objects.requireNonNull(checkpointIndexName, "checkpointIndexName must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        Objects.requireNonNull(clusterService, "clusterService must not be null");
        Objects.requireNonNull(maxConcurrentMappingUpdatesSetting, "maxConcurrentMappingUpdatesSetting must not be null");
        this.endpointResolver = ServiceLoader.load(TenantEndpointResolver.class).findFirst().orElseGet(DefaultTenantEndpointResolver::new);
        this.securityHeaders = new ThreadLocal<>();
        this.sdkClient = SdkClientProvider
            .buildSdkClient(
                client,
                xContentRegistry,
                threadPoolName,
                multiTenancyEnabled.get(settings),
                settings,
                remoteMetadataEndpoint,
                remoteMetadataServiceName,
                LOG
            );
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
        // if the index is a result index, we need to use the mapping from the local mapping file
        // otherwise, mappingUpToDate and settingUpToDate are always true in SDKDataManagement since
        // we require external CDK deployment to ensure that.
        return isResultIndex ? new IndexState(index.getMapping()) : new IndexState(index.getMapping(), true, true);
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
        if (!doesResultIndexExists(resultIndexOrAlias, tenantId) && !doesResultAliasExists(resultIndexOrAlias, tenantId)) {
            initCustomResultIndexDirectly(resultIndexOrAlias, ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    executeOnCustomIndex(resultIndexOrAlias, configId, user, roles, function, listener, tenantId);
                } else {
                    String error = "Creating custom result index with mappings call not acknowledged";
                    LOG.error(error);
                    listener.onFailure(new TimeSeriesException(error));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    executeOnCustomIndex(resultIndexOrAlias, configId, user, roles, function, listener, tenantId);
                } else {
                    listener.onFailure(exception);
                }
            }), tenantId);
        } else {
            validateResultIndexMapping(resultIndexOrAlias, ActionListener.wrap(validMapping -> {
                if (validMapping) {
                    executeOnCustomIndex(resultIndexOrAlias, configId, user, roles, function, listener, tenantId);
                } else {
                    listener.onFailure(new EndRunException("Result index mapping is not correct", true));
                }
            }, listener::onFailure), tenantId);
        }
    }

    private <T> void executeOnCustomIndex(
        String resultIndexOrAlias,
        String securityLogId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector(securityLogId, settings, securityHeaders)) {
            securityInjector.inject(user, roles);
            ActionListener<T> wrappedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
                securityInjector.close();
                listener.onFailure(e);
            });
            validateResultIndexAndExecute(resultIndexOrAlias, () -> {
                securityInjector.close();
                function.execute();
            }, true, wrappedListener, tenantId);
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
        // no setting to update in multi-tenancy environment since job storage is in eventbridge.
        if (Boolean.TRUE.equals(allMappingUpdated.get(tenantKey))) {
            return;
        }
        AtomicBoolean tenantUpdateRunning = updateRunning.computeIfAbsent(tenantKey, key -> new AtomicBoolean(false));
        AtomicInteger tenantUpdateRunningTimes = updateRunningTimes.computeIfAbsent(tenantKey, key -> new AtomicInteger(0));
        if (tenantUpdateRunningTimes.get() >= maxUpdateRunningTimes) {
            return;
        }
        // compareAndSet(false, true) makes the “check + set” atomic so only one thread wins.
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

    private void getConfigsWithCustomResultIndexAlias(String tenantId, ActionListener<List<String>> listener) {
        // Assuming config index is accessible via client
        if (configIndexName == null) {
            listener.onResponse(new ArrayList<>());
            return;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.filter(QueryBuilders.wildcardQuery(Config.RESULT_INDEX_FIELD, customResultIndexPrefix + "*"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10000).query(boolQuery);

        SearchDataObjectRequest sdkRequest = SearchDataObjectRequest
            .builder()
            .indices(new String[] { configIndexName })
            .searchSourceBuilder(sourceBuilder)
            .tenantId(tenantId)
            .build();

        try {
            sdkClient.searchDataObjectAsync(sdkRequest).whenComplete((response, throwable) -> {
                if (throwable != null) {
                    if (ExceptionUtil.isIndexNotFoundInMessage(throwable)) {
                        listener.onResponse(new ArrayList<>());
                    } else {
                        listener.onFailure(new TimeSeriesException("Fail to search configs via SDK", throwable));
                    }
                    return;
                }
                SearchResponse searchResponse;
                try {
                    searchResponse = response.searchResponse();
                } catch (Throwable t) {
                    if (isEmptySdkSearchParseFailure(t)) {
                        LOG.info("no config available.");
                        listener.onResponse(new ArrayList<>());
                    } else {
                        listener.onFailure(new TimeSeriesException("Fail to search configs via SDK", t));
                    }
                    return;
                }
                if (searchResponse == null || searchResponse.getHits() == null) {
                    LOG.info("no config available.");
                    listener.onResponse(new ArrayList<>());
                    return;
                }

                Iterator<SearchHit> iterator = searchResponse.getHits().iterator();

                Set<String> candidateResultIndices = new LinkedHashSet<>();
                while (iterator.hasNext()) {
                    SearchHit searchHit = iterator.next();
                    Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                    String resultIndex = (String) sourceAsMap.get(Config.RESULT_INDEX_FIELD);
                    if (resultIndex != null && resultIndex.startsWith(customResultIndexPrefix)) {
                        candidateResultIndices.add(resultIndex);
                    }
                }
                listener.onResponse(new ArrayList<>(candidateResultIndices));
            });
        } catch (Exception e) {
            if (e instanceof OpenSearchStatusException && ExceptionUtil.isIndexNotFoundInMessage(e)) {
                // zero-etl should create index when first detector is created
                listener.onResponse(new ArrayList<>());
            } else {
                listener.onFailure(e);
            }
        }
    }

    private static boolean isEmptySdkSearchParseFailure(Throwable t) {
        return t instanceof AssertionError && t.getMessage() != null && t.getMessage().contains("total: 0");
    }

    private void processResultIndexMappingIteration(
        int indexPos,
        List<String> candidateResultIndices,
        String tenantId,
        GroupedActionListener<Void> conglomerateListeneer
    ) {
        if (indexPos >= candidateResultIndices.size()) {
            return;
        }
        String index = candidateResultIndices.get(indexPos);

        LOG.info(new ParameterizedMessage("Check [{}]'s mapping", index));

        validateResultIndexMapping(index, ActionListener.wrap(valid -> {
            if (!valid) {
                updateMapping(index, tenantId, ActionListener.wrap(response -> {
                    conglomerateListeneer.onResponse(null);
                    processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
                }, e -> {
                    LOG.error("Fail to update mapping for " + index, e);
                    conglomerateListeneer.onFailure(e);
                    processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
                }));
            } else {
                conglomerateListeneer.onResponse(null);
                processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
            }
        }, e -> {
            LOG.error("Fail to validate mapping for " + index, e);
            conglomerateListeneer.onFailure(e);
            processResultIndexMappingIteration(indexPos + 1, candidateResultIndices, tenantId, conglomerateListeneer);
        }), tenantId);
    }

    private void updateMapping(String index, String tenantId, ActionListener<Boolean> listener) {
        try {
            String body = resultMapping;
            Response response = sendWithBody("PUT", "/" + index + "/_mapping", body, tenantId);
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
        // return true to avoid creating a default result index in places like IndexMemoryPressureAwareResultHandler.flush.
        return true;
    }

    @Override
    public boolean doesResultIndexExists(String indexName, String tenantId) {
        return headRequest("/" + indexName, tenantId);
    }

    @Override
    public boolean doesResultAliasExists(String aliasName, String tenantId) {
        return headRequest("/_alias/" + aliasName, tenantId);
    }

    @Override
    public int getSchemaVersion(IndexType index) {
        return indexStates.computeIfAbsent(index, k -> newIndexStates(k)).schemaVersion;
    }

    /**
     * Get the rest client for the given tenant id from the shared cache in {@link RestClientProvider}.
     * Reuses clients per endpoint so we don’t re-resolve and re-handshake (TCP/TLS) on every call.
     *
     * @param tenantId tenant id
     * @return the rest client for the given tenant id
     * @throws RuntimeException if failed to create the rest client for the given tenant id
     */
    private RestClient getRestClient(String tenantId) {
        String endpoint = endpointResolver.resolve(tenantId);
        return RestClientProvider.getRestClient(endpoint);
    }

    private void applySecurityHeaders(Request request) {
        Map<String, String> headers = securityHeaders.get();
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(optionsBuilder::addHeader);
        }
        request.setOptions(optionsBuilder.build());
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
        if (response == null) {
            return "null response";
        }
        String status = response.getStatusLine() == null ? "null" : Integer.toString(response.getStatusLine().getStatusCode());
        String body = getResponseBody(response);
        return "status=" + status + ", body=" + body;
    }

    private boolean headRequest(String path, String tenantId) {
        try {
            Request request = new Request("HEAD", path);
            applySecurityHeaders(request);
            Response response = getRestClient(tenantId).performRequest(request);
            return is2xx(response);
        } catch (Exception e) {
            LOG.warn("Failed to check existence for path {}", path, e);
            return false;
        }
    }

    private Response sendWithBody(String method, String path, String body, String tenantId) {
        try {
            Request request = new Request(method, path);
            if (body != null) {
                request.setJsonEntity(body);
            }
            applySecurityHeaders(request);
            return getRestClient(tenantId).performRequest(request);
        } catch (Exception e) {
            LOG.warn("Failed to {} {}", method, path, e);
            return null;
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
        if (!mappingValidated) {
            validateResultIndexMapping(resultIndexOrAlias, ActionListener.wrap(validMapping -> {
                if (validMapping) {
                    executeAfterValidateResultIndexMapping(resultIndexOrAlias, function, listener, tenantId);
                } else {
                    LOG.warn("Can't create analysis with custom result index {} as its mapping is invalid", resultIndexOrAlias);
                    listener.onFailure(new IllegalArgumentException(CommonMessages.INVALID_RESULT_INDEX_MAPPING + resultIndexOrAlias));
                }
            }, listener::onFailure), tenantId);
        } else {
            try {
                executeAfterValidateResultIndexMapping(resultIndexOrAlias, function, listener, tenantId);
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
    public void validateResultIndexMapping(String resultIndexOrAlias, ActionListener<Boolean> thenDo, String tenantId) {
        getConcreteIndex(resultIndexOrAlias, ActionListener.wrap(concreteIndex -> {
            try {
                initResultMapping();
                if (resultFieldConfigs == null) {
                    thenDo.onResponse(false);
                    return;
                }

                Request mappingRequest = new Request("GET", "/" + concreteIndex + "/_mapping");
                applySecurityHeaders(mappingRequest);

                getRestClient(tenantId).performRequestAsync(mappingRequest, new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        if (is2xx(response)) {
                            try {
                                String body = getResponseBody(response);
                                Map<String, Object> responseMap = XContentHelper
                                    .convertToMap(new BytesArray(body), false, XContentType.JSON)
                                    .v2();
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
                        } else {
                            LOG
                                .warn(
                                    "Failed to get mapping for index {}. Status: {}",
                                    concreteIndex,
                                    response.getStatusLine().getStatusCode()
                                );
                            thenDo.onResponse(false);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOG.error("Failed to get mapping for index " + concreteIndex, e);
                        thenDo.onFailure(new RuntimeException(e));
                    }
                });

            } catch (Exception e) {
                LOG.error("Failed to validate result index mapping for index " + concreteIndex, e);
                thenDo.onResponse(false);
            }
        }, thenDo::onFailure));
    }

    private void getConcreteIndex(String indexOrAliasName, ActionListener<String> thenDo) {
        getConcreteIndex(indexOrAliasName, thenDo, null);
    }

    private void getConcreteIndex(String indexOrAliasName, ActionListener<String> thenDo, String tenantId) {
        // First check if alias exists before attempting to resolve
        if (doesResultAliasExists(indexOrAliasName, tenantId)) {
            Request request = new Request("GET", "/_alias/" + indexOrAliasName);
            applySecurityHeaders(request);

            getRestClient(tenantId).performRequestAsync(request, new ResponseListener() {
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
            // If this is not an alias or the index does not exist yet, return indexOrAliasName
            thenDo.onResponse(indexOrAliasName);
        }
    }

    private <T> void executeAfterValidateResultIndexMapping(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        RestClient restClient = getRestClient(tenantId);

        // Write dummy doc with a unique id to avoid concurrent delete collisions.
        final String requestDummyId = dummyResultId + "-" + UUID.randomUUID();
        Request writeRequest = new Request("PUT", "/" + resultIndexOrAlias + "/_doc/" + requestDummyId);
        writeRequest.setJsonEntity(dummyResultBody);
        applySecurityHeaders(writeRequest);

        restClient.performRequestAsync(writeRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response writeResponse) {
                if (is2xx(writeResponse)) {
                    LOG.debug("Successfully wrote dummy result to result index {}", resultIndexOrAlias);

                    // Delete dummy doc
                    Request deleteRequest = new Request("DELETE", "/" + resultIndexOrAlias + "/_doc/" + requestDummyId);
                    applySecurityHeaders(deleteRequest);

                    restClient.performRequestAsync(deleteRequest, new ResponseListener() {
                        @Override
                        public void onSuccess(Response deleteResponse) {
                            if (is2xx(deleteResponse)) {
                                LOG.info("Successfully deleted dummy result from result index {}", resultIndexOrAlias);
                                try {
                                    function.execute();
                                } catch (Exception e) {
                                    listener.onFailure(e);
                                }
                            } else {
                                String error = "Failed to delete dummy result from result index "
                                    + resultIndexOrAlias
                                    + ". Status: "
                                    + deleteResponse.getStatusLine().getStatusCode();
                                LOG.error(error);
                                listener.onFailure(new RuntimeException(error));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            LOG
                                .error(
                                    "Failed to delete dummy result from result index {}. Write response: {}",
                                    resultIndexOrAlias,
                                    formatResponseDetails(writeResponse),
                                    e
                                );
                            listener.onFailure(new RuntimeException(e));
                        }
                    });

                } else {
                    String error = "Failed to write dummy result to result index "
                        + resultIndexOrAlias
                        + ". Status: "
                        + writeResponse.getStatusLine().getStatusCode();
                    LOG.error("{}; response: {}", error, formatResponseDetails(writeResponse));
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

    @Override
    public void initCustomResultIndexDirectly(
        String resultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId
    ) {
        try {
            // Generate the actual index name using the pattern (e.g., <alias-history-{now/d}-1>)
            String indexName = TimeSeriesIndex.getCustomResultIndexPattern(resultIndexAlias);
            String encodedIndexName = encodeDateMathIndexName(indexName);
            // Create index with mapping and alias
            String body = "{\"mappings\":" + resultMapping + ",\"aliases\":{\"" + resultIndexAlias + "\":{}}}";
            Response response = sendWithBody("PUT", "/" + encodedIndexName, body, tenantId);
            if (response != null && is2xx(response)) {
                actionListener.onResponse(new CreateIndexResponse(true, true, indexName));
            } else if (isResourceAlreadyExists(response)) {
                actionListener.onFailure(new ResourceAlreadyExistsException(indexName));
            } else {
                String error = "Creating result index "
                    + indexName
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
        if (!doesResultIndexExists(resultIndexOrAlias, tenantId) && !doesResultAliasExists(resultIndexOrAlias, tenantId)) {
            initCustomResultIndexDirectly(resultIndexOrAlias, ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    LOG.info("Successfully created result index {}", resultIndexOrAlias);
                    validateResultIndexAndExecute(resultIndexOrAlias, function, true, listener, tenantId);
                } else {
                    // Creation may fail if another node created the index concurrently; treat as already exists.
                    if (doesResultIndexExists(resultIndexOrAlias, tenantId) || doesResultAliasExists(resultIndexOrAlias, tenantId)) {
                        LOG.info("Result index {} already exists after create attempt, validating mapping", resultIndexOrAlias);
                        validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId);
                    } else {
                        String error = "Creating result index with mappings call not acknowledged: " + resultIndexOrAlias;
                        LOG.error(error);
                        listener.onFailure(new EndRunException(error, false));
                    }
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId);
                } else if (doesResultIndexExists(resultIndexOrAlias, tenantId) || doesResultAliasExists(resultIndexOrAlias, tenantId)) {
                    LOG.info("Result index {} already exists after create attempt, validating mapping", resultIndexOrAlias);
                    validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId);
                } else {
                    LOG.error("Failed to create result index " + resultIndexOrAlias, exception);
                    listener.onFailure(exception);
                }
            }), tenantId);
        } else {
            validateResultIndexAndExecute(resultIndexOrAlias, function, false, listener, tenantId);
        }
    }

    @Override
    public void initFlattenedResultIndex(
        String flattenedResultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId
    ) {
        try {
            String indexName = TimeSeriesIndex.getCustomResultIndexPattern(flattenedResultIndexAlias);
            String encodedIndexName = encodeDateMathIndexName(indexName);
            StringBuilder bodyBuilder = new StringBuilder();
            bodyBuilder.append("{\"mappings\":").append(IndexResourceLoader.getFlattenedResultMappingsFromContent(resultMapping));
            if (flattenedResultIndexAlias != null) {
                bodyBuilder.append(",\"aliases\":{").append("\"").append(flattenedResultIndexAlias).append("\":{}}");
            }
            bodyBuilder.append("}");

            Response response = sendWithBody("PUT", "/" + encodedIndexName, bodyBuilder.toString(), tenantId);
            if (response != null && is2xx(response)) {
                actionListener.onResponse(new CreateIndexResponse(true, true, indexName));
            } else if (isResourceAlreadyExists(response)) {
                actionListener.onFailure(new ResourceAlreadyExistsException(indexName));
            } else {
                String errorMsg = "Index creation not acknowledged for index: " + indexName;
                LOG.error(errorMsg);
                actionListener.onFailure(new IllegalStateException(errorMsg));
            }
        } catch (Exception e) {
            LOG.error("Error while initializing flattened result index: {}", flattenedResultIndexAlias, e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Encode date math index names (e.g., &lt;alias-{now/d}-1&gt;) so the REST client URI builder accepts them.
     * 
     * @param indexName the index name to encode
     * @return the encoded index name
     */
    private String encodeDateMathIndexName(String indexName) {
        return URLEncoder.encode(indexName, StandardCharsets.UTF_8);
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
