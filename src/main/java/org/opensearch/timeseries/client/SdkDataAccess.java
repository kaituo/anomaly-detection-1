/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Build;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.index.reindex.AbstractBulkByScrollRequest;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.remote.metadata.client.BulkDataObjectRequest;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.SdkStateManager;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver;
import org.opensearch.timeseries.util.CrossClusterConfigUtils;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.TransportUtil;

/**
 * Data access layer backed by the multiple clients (e.g., s3 or remote metadata SDK client).
 * This class assigns calls to the appropriate client based on index name
 *
 * Instead of replacing the client calls everywhere with different client calls, this class
 * assigns calls to the appropriate client based on index name, which is less intrusive and safer.
 * For example, config index or state index goes to remote metadata SDK client,
 * while checkpoint index goes to s3 client).
 * 
 * This also helps maintainability as people unfamiliar with the multitenant feature
 * can still write code for multitenant environment without knowing the details of the multitenant feature.
 * 
 * Another method is to intercept the client calls in transport layer and convert them to various clients.
 * This method is less scalable as it requires two hops (transport layer -> interceptor -> various clients).
 * Here we have one hop (call sites -> target service).
 * 
 * TODO: add security support (e.g., add tenant id to thread context).
 */
public class SdkDataAccess implements DataAccess {

    private static final Logger LOG = LogManager.getLogger(SdkDataAccess.class);
    private static final int NODES_INFO_CONNECT_TIMEOUT_MILLIS = 200;
    private static final Pattern UPDATE_SCRIPT_PATTERN = Pattern.compile("ctx\\._source\\.([\\w\\.]+)\\s*=\\s*(.+?);?$");

    private final SdkClient sdkClient;
    private final ClusterService clusterService;
    private final Settings settings;
    private final TenantEndpointResolver endpointResolver;
    private final ThreadLocal<Map<String, String>> securityHeaders;
    private final SdkStateManager stateManager;
    private final ConfigDocumentStore configDocumentStore;

    public SdkDataAccess(
        SdkClient sdkClient,
        ClusterService clusterService,
        Settings settings,
        SdkStateManager stateManager,
        ConfigDocumentStore configDocumentStore,
        TenantEndpointResolver endpointResolver
    ) {
        this.sdkClient = Objects.requireNonNull(sdkClient, "sdkClient must not be null");

        this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
        this.securityHeaders = new ThreadLocal<>();
        this.stateManager = Objects.requireNonNull(stateManager, "stateManager must not be null");
        this.configDocumentStore = Objects.requireNonNull(configDocumentStore, "configDocumentStore must not be null");
    }

    @Override
    public void search(SearchRequest request, TenantContext tenantContext, ActionListener<SearchResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        searchInternal(request, tenantContext.getTenantId(), listener, !tenantContext.isSystemWide());
    }

    @Override
    public void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        try {
            String[] requestedNodeIds = request.nodesIds();
            List<DiscoveryNode> targetNodes = new ArrayList<>();

            for (String nodeId : requestedNodeIds) {
                // nodeId in multi-tenancy is the IP address and port of the node in the format "ip:port"
                DiscoveryNode node = TransportUtil.createDiscoveryNodeFromIpPort(nodeId);
                if (node != null) {
                    targetNodes.add(node);
                }
            }

            // The multitenant SDK path cannot call transport-admin APIs. An alternative would be to branch
            // on multi-tenancy and skip nodesInfo entirely, but that would force callers (e.g., HashRing)
            // to add their own multi-tenancy checks. We avoid spreading multi-tenancy conditionals across the
            // codebase to keep callers simple, reduce drift between single- and multi-tenant behavior, and keep
            // backend swaps (transport vs SDK, etc.) localized to DataAccess rather than every call site. That way,
            // we update this abstraction once rather than modifying every consumer.
            // We synthesize a NodesInfoResponse so callers can run the same logic regardless of
            // storage backend and still see our plugin/version on each node.
            List<NodeInfo> nodeInfos = targetNodes
                .stream()
                .filter(this::isNodeEndpointReady)
                .map(this::buildNodeInfo)
                .collect(Collectors.toList());
            ClusterName clusterName = clusterService.getClusterName() == null ? ClusterName.DEFAULT : clusterService.getClusterName();
            NodesInfoResponse response = new NodesInfoResponse(clusterName, nodeInfos, Collections.emptyList());
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean isNodeEndpointReady(DiscoveryNode node) {
        String nodeId = node.getId();
        int separator = nodeId.lastIndexOf(':');
        if (separator <= 0 || separator == nodeId.length() - 1) {
            LOG.debug("Skip readiness probe for invalid multi-tenant node id [{}]", nodeId);
            return false;
        }

        String host = nodeId.substring(0, separator);
        int port = Integer.parseInt(nodeId.substring(separator + 1));
        try (Socket socket = new Socket()) {
            InetAddress address = InetAddress.getByName(host);
            socket.connect(new InetSocketAddress(address, port), NODES_INFO_CONNECT_TIMEOUT_MILLIS);
            return true;
        } catch (IOException e) {
            LOG.debug("Node endpoint is not ready yet [{}]", nodeId, e);
            return false;
        }
    }

    @Override
    public void searchWithInjectedSecurity(
        SearchRequest request,
        User user,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.getTenantId();
        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("search", settings, securityHeaders)) {
            if (user != null) {
                securityInjector.inject(user.getName(), user.getRoles());
            }
            searchInternal(request, tenantId, listener, validateTenantId);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void searchWithInjectedSecurity(
        SearchRequest request,
        String configId,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.getTenantId();
        if (configId == null || context == null) {
            searchInternal(request, tenantId, listener, validateTenantId);
            return;
        }

        stateManager.getConfig(configId, tenantId, context, true, ActionListener.wrap(configOp -> {
            if (!configOp.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config is not available.", false));
                return;
            }
            Config config = configOp.get();
            User configUser = SecurityUtil.getUserFromConfig(config, settings);
            try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("search", settings, securityHeaders)) {
                securityInjector.inject(configUser.getName(), configUser.getRoles());
                searchInternal(request, tenantId, listener, validateTenantId);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    public static String[] extractSourceIndices(SearchRequest request) {
        String[] indices = request.indices();
        if (indices == null || indices.length == 0) {
            return new String[0];
        }
        return indices;
    }

    private void searchInternal(SearchRequest request, String tenantId, ActionListener<SearchResponse> listener, boolean validateTenantId) {
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        SearchSourceBuilder sourceBuilder = request.source();
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
            request.source(sourceBuilder);
        }

        String[] indices = extractSourceIndices(request);
        if (IndexUtils.shouldUseRestSearch(indices)) {
            executeRestSearch(request, tenantId, listener);
            return;
        }

        SearchDataObjectRequest sdkRequest = SearchDataObjectRequest
            .builder()
            .indices(indices)
            .tenantId(tenantId)
            .searchSourceBuilder(sourceBuilder)
            .build();

        try {
            LOG.info("SDK search submit tenant={} indices={}", tenantId, (Object) sdkRequest.indices());
            sdkClient.searchDataObjectAsync(sdkRequest).whenComplete((response, throwable) -> {
                LOG
                    .info(
                        "SDK search future complete tenant={} indices={} throwable={} responseNull={}",
                        tenantId,
                        (Object) sdkRequest.indices(),
                        throwable != null,
                        response == null
                    );
                if (throwable != null) {
                    if (ExceptionUtil.isIndexNotFoundInMessage(throwable)) {
                        LOG
                            .info(
                                "SDK search treating index-not-found as empty response tenant={} indices={}",
                                tenantId,
                                (Object) sdkRequest.indices()
                            );
                        listener.onResponse(emptySearchResponse());
                    } else {
                        LOG.error("SDK search future failed tenant={} indices={}", tenantId, (Object) sdkRequest.indices(), throwable);
                        listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                    }
                    return;
                }

                try {
                    SearchResponse searchResponse = parseSdkSearchResponse(response, tenantId, sdkRequest.indices());
                    long totalHits = searchResponse.getHits() == null || searchResponse.getHits().getTotalHits() == null
                        ? -1
                        : searchResponse.getHits().getTotalHits().value();
                    LOG
                        .info(
                            "SDK search forwarding response tenant={} indices={} totalHits={}",
                            tenantId,
                            (Object) sdkRequest.indices(),
                            totalHits
                        );
                    LOG.debug("SDK search complete for indices {}", (Object) sdkRequest.indices());
                    listener.onResponse(searchResponse);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            if (ExceptionUtil.isIndexNotFoundInMessage(e)) {
                // zero-etl should create index when first detector is created
                listener.onResponse(emptySearchResponse());
            } else {
                listener.onFailure(e);
            }
        }
    }

    private static SearchResponse emptySearchResponse() {
        return new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            0,
            0,
            0,
            0,
            new SearchResponse.PhaseTook(Collections.emptyMap()),
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY,
            null
        );
    }

    private SearchResponse parseSdkSearchResponse(SearchDataObjectResponse response, String tenantId, String[] indices) {
        if (response == null) {
            LOG.error("SDK search future returned null response tenant={} indices={}", tenantId, (Object) indices);
            throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR);
        }

        boolean parserNull = response.parser() == null;
        LOG.info("SDK search parsing response tenant={} indices={} parserNull={}", tenantId, (Object) indices, parserNull);

        SearchResponse searchResponse;
        try {
            searchResponse = response.searchResponse();
        } catch (Throwable t) {
            if (isEmptySdkSearchParseFailure(t)) {
                LOG
                    .warn(
                        "SDK search treating empty parse failure as empty response tenant={} indices={} cause={}: {}",
                        tenantId,
                        (Object) indices,
                        t.getClass().getSimpleName(),
                        t.getMessage()
                    );
                return emptySearchResponse();
            }
            LOG.error("SDK search response parse failed tenant={} indices={} parserNull={}", tenantId, (Object) indices, parserNull, t);
            throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR, t);
        }

        if (searchResponse == null) {
            LOG.error("SDK search future returned null SearchResponse tenant={} indices={}", tenantId, (Object) indices);
            throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR);
        }
        return searchResponse;
    }

    private static boolean isEmptySdkSearchParseFailure(Throwable t) {
        return t instanceof AssertionError && t.getMessage() != null && t.getMessage().contains("total: 0");
    }

    private void executeRestSearch(SearchRequest request, String tenantId, ActionListener<SearchResponse> listener) {
        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("search", settings, securityHeaders)) {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildSearchRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(SearchResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionUtil.isIndexNotFoundInMessage(e)) {
                        listener.onResponse(emptySearchResponse());
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildSearchRestRequest(SearchRequest request) throws IOException {
        String[] indices = request.indices();
        String path = (indices == null || indices.length == 0) ? "/_search" : "/" + joinIndices(indices) + "/_search";
        Request restRequest = new Request("POST", path);
        applyIndicesOptions(restRequest, request.indicesOptions());
        // Required when parsing the REST response into SearchResponse (aggregations/suggesters need type info)
        restRequest.addParameter("typed_keys", "true");
        SearchSourceBuilder sourceBuilder = request.source();
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
            request.source(sourceBuilder);
        }
        XContentBuilder builder = XContentFactory.jsonBuilder();
        sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(BytesReference.bytes(builder)), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    @Override
    public void updateByQuery(UpdateByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        SearchRequest searchRequest = request.getSearchRequest();
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
            searchRequest.source(sourceBuilder);
        }

        String[] indices = extractSourceIndices(searchRequest);
        if (IndexUtils.shouldUseRestSearch(indices)) {
            executeRestUpdateByQuery(request, tenantId, listener);
            return;
        }

        SearchDataObjectRequest sdkRequest = SearchDataObjectRequest
            .builder()
            .indices(indices)
            .tenantId(tenantId)
            .searchSourceBuilder(sourceBuilder)
            .build();

        sdkClient.searchDataObjectAsync(sdkRequest).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            SearchResponse searchResponse;
            try {
                searchResponse = parseSdkSearchResponse(response, tenantId, sdkRequest.indices());
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (searchResponse.getHits() == null) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
            List<SearchHit> limitedHits = hits;
            if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES && hits.size() > request.getMaxDocs()) {
                limitedHits = hits.subList(0, request.getMaxDocs());
            }

            if (limitedHits.isEmpty()) {
                listener.onResponse(buildBulkByScrollResponse(request, 0, 0, 0, Collections.emptyList(), TimeValue.ZERO));
                return;
            }

            Map<String, Object> updatedFields = extractUpdateFields(request.getScript());
            if (updatedFields.isEmpty()) {
                listener.onFailure(new OpenSearchStatusException("No update fields derived from script", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            updateDocumentsSequentially(limitedHits, updatedFields, tenantId, request, 0, 0, listener);
        });
    }

    private void updateDocumentsSequentially(
        List<SearchHit> hits,
        Map<String, Object> updatedFields,
        String tenantId,
        UpdateByQueryRequest updateRequest,
        int index,
        long updatedCount,
        ActionListener<BulkByScrollResponse> listener
    ) {
        if (index >= hits.size()) {
            listener
                .onResponse(
                    buildBulkByScrollResponse(updateRequest, hits.size(), updatedCount, 0, Collections.emptyList(), TimeValue.ZERO)
                );
            return;
        }

        SearchHit hit = hits.get(index);

        UpdateDataObjectRequest sdkUpdateRequest = UpdateDataObjectRequest
            .builder()
            .index(hit.getIndex())
            .id(hit.getId())
            .tenantId(tenantId)
            .retryOnConflict(updateRequest.getMaxRetries())
            .dataObject(updatedFields)
            .refreshPolicy(updateRequest.isRefresh() ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE)
            .timeout(updateRequest.getTimeout())
            .build();

        sdkClient.updateDataObjectAsync(sdkUpdateRequest).whenComplete((updateResponse, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            try {
                UpdateResponse.fromXContent(updateResponse.parser());
            } catch (Exception e) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse update response", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            updateDocumentsSequentially(hits, updatedFields, tenantId, updateRequest, index + 1, updatedCount + 1, listener);
        });
    }

    private Map<String, Object> extractUpdateFields(Script script) {
        if (script == null || script.getIdOrCode() == null) {
            return Collections.emptyMap();
        }

        Matcher matcher = UPDATE_SCRIPT_PATTERN.matcher(script.getIdOrCode().trim());
        if (!matcher.find()) {
            return Collections.emptyMap();
        }

        String field = matcher.group(1);
        String rawValue = stripTrailingSemicolon(matcher.group(2).trim());

        Object value;
        if (rawValue.startsWith("params.")) {
            String paramKey = rawValue.substring("params.".length());
            Map<String, Object> params = script.getParams();
            if (params == null || !params.containsKey(paramKey)) {
                return Collections.emptyMap();
            }
            value = params.get(paramKey);
        } else {
            value = parseLiteral(rawValue);
        }

        return Collections.singletonMap(field, value);
    }

    private Object parseLiteral(String value) {
        if ("null".equalsIgnoreCase(value)) {
            return null;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.valueOf(value);
        }
        if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private String stripTrailingSemicolon(String value) {
        if (value.endsWith(";")) {
            return value.substring(0, value.length() - 1).trim();
        }
        return value;
    }

    private BulkByScrollResponse buildBulkByScrollResponse(
        AbstractBulkByScrollRequest<?> request,
        long total,
        long updated,
        long deleted,
        List<BulkItemResponse.Failure> failures,
        TimeValue took
    ) {
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            null,
            total,
            updated,
            0,
            deleted,
            total > 0 ? 1 : 0,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            request.getRequestsPerSecond(),
            null,
            TimeValue.ZERO
        );

        return new BulkByScrollResponse(took, status, failures, Collections.<ScrollableHitSource.SearchFailure>emptyList(), false);
    }

    @Override
    public void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (isConfigIndex(request.index())) {
            configDocumentStore.delete(request, tenantContext, listener);
            return;
        }

        if (IndexUtils.shouldUseRestSearch(request.index())) {
            executeRestDelete(request, tenantId, listener);
            return;
        }

        DeleteDataObjectRequest.Builder builder = DeleteDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }

        sdkClient.deleteDataObjectAsync(builder.build()).whenComplete((deleteResponse, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            try {
                listener.onResponse(DeleteResponse.fromXContent(deleteResponse.parser()));
            } catch (Exception e) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse delete response", RestStatus.INTERNAL_SERVER_ERROR));
            }
        });
    }

    @Override
    public void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (isConfigIndex(request.index())) {
            configDocumentStore.index(request, tenantContext, listener);
            return;
        }

        if (IndexUtils.shouldUseRestSearch(request.index())) {
            executeRestIndex(request, tenantId, listener);
            return;
        }

        Map<String, Object> sourceAsMap;
        try {
            sourceAsMap = request.sourceAsMap();
        } catch (Exception e) {
            listener.onFailure(new OpenSearchStatusException("Index request missing document", RestStatus.BAD_REQUEST));
            return;
        }

        boolean overwriteIfExists = request.opType() != IndexRequest.OpType.CREATE;
        PutDataObjectRequest.Builder builder = PutDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .dataObject(sourceAsMap)
            .overwriteIfExists(overwriteIfExists)
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }

        sdkClient.putDataObjectAsync(builder.build()).whenComplete((putResponse, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            try {
                listener.onResponse(IndexResponse.fromXContent(putResponse.parser()));
            } catch (Exception e) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse index response", RestStatus.INTERNAL_SERVER_ERROR));
            }
        });
    }

    @Override
    public void bulk(BulkRequest request, TenantContext tenantContext, ActionListener<BulkResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.getTenantId();

        if (tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (IndexUtils.shouldUseRestSearch(request.getIndices().toArray(new String[0]))) {
            executeRestBulk(request, tenantId, listener);
            return;
        }

        BulkDataObjectRequest sdkBulkRequest = BulkDataObjectRequest.builder().build();
        sdkBulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        sdkBulkRequest.timeout(request.timeout());

        for (DocWriteRequest<?> docWriteRequest : request.requests()) {
            try {
                if (docWriteRequest instanceof IndexRequest) {
                    sdkBulkRequest.add(convertIndexRequest((IndexRequest) docWriteRequest, tenantId));
                } else if (docWriteRequest instanceof UpdateRequest) {
                    sdkBulkRequest.add(convertUpdateRequest((UpdateRequest) docWriteRequest, tenantId));
                } else if (docWriteRequest instanceof DeleteRequest) {
                    sdkBulkRequest.add(convertDeleteRequest((DeleteRequest) docWriteRequest, tenantId));
                } else {
                    listener
                        .onFailure(
                            new OpenSearchStatusException(
                                "Unsupported bulk request type: " + docWriteRequest.getClass().getSimpleName(),
                                RestStatus.BAD_REQUEST
                            )
                        );
                    return;
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }

        sdkClient.bulkDataObjectAsync(sdkBulkRequest).whenComplete((bulkResponse, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            BulkResponse response = bulkResponse.bulkResponse();
            if (response == null) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse bulk response", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }
            listener.onResponse(response);
        });
    }

    private PutDataObjectRequest convertIndexRequest(IndexRequest request, String tenantId) {
        Map<String, Object> sourceAsMap = request.sourceAsMap();
        if (sourceAsMap == null) {
            throw new OpenSearchStatusException("Index request missing document", RestStatus.BAD_REQUEST);
        }

        boolean overwriteIfExists = request.opType() != IndexRequest.OpType.CREATE;
        PutDataObjectRequest.Builder builder = PutDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .dataObject(sourceAsMap)
            .overwriteIfExists(overwriteIfExists)
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }
        return builder.build();
    }

    @Override
    public void deleteByQuery(DeleteByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.getTenantId();
        if (!tenantContext.isSystemWide() && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (tenantContext.isSystemWide()) {
            // For system-wide operations, paginate until all matching documents are deleted
            deleteByQueryWithPagination(request, tenantId, 0, 0, TimeValue.ZERO, listener);
        } else {
            // For user-scoped operations, do a single delete by query
            deleteByQueryInternal(request, tenantId, listener);
        }
    }

    /**
     * Recursively deletes documents matching the query until no more are found.
     * Accumulates deleted counts and time across iterations.
     */
    private void deleteByQueryWithPagination(
        DeleteByQueryRequest request,
        String tenantId,
        long totalDeleted,
        long totalSearched,
        TimeValue totalTook,
        ActionListener<BulkByScrollResponse> listener
    ) {
        deleteByQueryInternal(request, tenantId, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onResponse(buildBulkByScrollResponse(request, totalSearched, 0, totalDeleted, Collections.emptyList(), totalTook));
                return;
            }

            // Check for failures
            if (hasDeleteByQueryFailures(response)) {
                listener.onFailure(new OpenSearchStatusException("Failed to delete by query", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            long deleted = response.getDeleted();
            long newTotalDeleted = totalDeleted + deleted;
            long newTotalSearched = totalSearched + response.getTotal();
            TimeValue newTotalTook = TimeValue.timeValueMillis(totalTook.millis() + response.getTook().millis());

            if (deleted > 0) {
                // More documents may exist, continue pagination
                deleteByQueryWithPagination(request, tenantId, newTotalDeleted, newTotalSearched, newTotalTook, listener);
            } else {
                // No more documents to delete
                listener
                    .onResponse(
                        buildBulkByScrollResponse(request, newTotalSearched, 0, newTotalDeleted, Collections.emptyList(), newTotalTook)
                    );
            }
        }, listener::onFailure));
    }

    private boolean hasDeleteByQueryFailures(BulkByScrollResponse response) {
        return response.isTimedOut()
            || (response.getBulkFailures() != null && !response.getBulkFailures().isEmpty())
            || (response.getSearchFailures() != null && !response.getSearchFailures().isEmpty());
    }

    /**
     * Executes a single delete by query operation (no pagination).
     */
    private void deleteByQueryInternal(DeleteByQueryRequest request, String tenantId, ActionListener<BulkByScrollResponse> listener) {
        // remote metadata SDK client does not support delete by query, so we need to use search and delete separately.
        SearchRequest searchRequest = request.getSearchRequest();
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
            searchRequest.source(sourceBuilder);
        }

        String[] indices = extractSourceIndices(searchRequest);
        if (IndexUtils.shouldUseRestSearch(indices)) {
            executeRestDeleteByQuery(request, tenantId, listener);
            return;
        }

        SearchDataObjectRequest sdkRequest = SearchDataObjectRequest
            .builder()
            .indices(indices)
            .tenantId(tenantId)
            .searchSourceBuilder(sourceBuilder)
            .build();

        sdkClient.searchDataObjectAsync(sdkRequest).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            SearchResponse searchResponse;
            try {
                searchResponse = parseSdkSearchResponse(response, tenantId, sdkRequest.indices());
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (searchResponse.getHits() == null) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
            List<SearchHit> limitedHits = hits;
            if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES && hits.size() > request.getMaxDocs()) {
                limitedHits = hits.subList(0, request.getMaxDocs());
            }

            if (limitedHits.isEmpty()) {
                listener.onResponse(buildBulkByScrollResponse(request, 0, 0, 0, Collections.emptyList(), TimeValue.ZERO));
                return;
            }

            BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder().build();
            bulkRequest.setRefreshPolicy(request.isRefresh() ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE);
            bulkRequest.timeout(request.getTimeout());

            for (SearchHit hit : limitedHits) {
                DeleteDataObjectRequest.Builder deleteBuilder = DeleteDataObjectRequest
                    .builder()
                    .index(hit.getIndex())
                    .id(hit.getId())
                    .tenantId(tenantId);
                bulkRequest.add(deleteBuilder.build());
            }

            sdkClient.bulkDataObjectAsync(bulkRequest).whenComplete((bulkResponse, bulkThrowable) -> {
                if (bulkThrowable != null) {
                    listener.onFailure(SdkClientUtils.unwrapAndConvertToException(bulkThrowable));
                    return;
                }

                BulkResponse bulkSdkResponse = bulkResponse.bulkResponse();
                if (bulkSdkResponse == null) {
                    listener
                        .onFailure(new OpenSearchStatusException("Failed to parse bulk delete response", RestStatus.INTERNAL_SERVER_ERROR));
                    return;
                }
                BulkItemResponse[] items = bulkSdkResponse.getItems();
                List<BulkItemResponse.Failure> failures = items == null
                    ? Collections.emptyList()
                    : Arrays.stream(items).map(BulkItemResponse::getFailure).filter(Objects::nonNull).collect(Collectors.toList());
                long deletedCount = items == null ? 0 : items.length - failures.size();
                TimeValue took = bulkSdkResponse.getTook() == null ? TimeValue.ZERO : bulkSdkResponse.getTook();
                listener.onResponse(buildBulkByScrollResponse(request, hits.size(), 0, deletedCount, failures, took));
            });
        });
    }

    @Override
    public void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (isConfigIndex(request.index())) {
            configDocumentStore.get(request, tenantContext, listener);
            return;
        }

        if (IndexUtils.shouldUseRestSearch(request.index())) {
            executeRestGet(request, tenantId, listener);
            return;
        }

        GetDataObjectRequest sdkRequest = GetDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .fetchSourceContext(request.fetchSourceContext())
            .build();

        sdkClient.getDataObjectAsync(sdkRequest).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }

            GetResponse getResponse = response.getResponse();
            if (getResponse == null) {
                try {
                    getResponse = GetResponse.fromXContent(response.parser());
                } catch (Exception e) {
                    listener.onFailure(new OpenSearchStatusException("Failed to parse get response", RestStatus.INTERNAL_SERVER_ERROR));
                    return;
                }
            }
            listener.onResponse(getResponse);
        });
    }

    /**
     * Retrieves field mappings in two modes depending on the target cluster.
     * Local indexes call the standard field mapping API ({@code /{index}/_mapping/field/{fields}}),
     * while remote indexes call the field caps API ({@code /{cluster}:{index}/_field_caps}) because the
     * mapping API is not available over remote connections. Both responses are normalized into
     * the plugin-owned {@link FieldMappingsView}.
     * 
     * @param request the get field mappings request
     * @param user the user
     * @param tenantContext the tenant context
     * @param clusterName the cluster name
     * @param context the context (e.g., AD or Forecast)
     * @param listener the listener
     */
    @Override
    public void getFieldMappings(
        GetFieldMappingsRequest request,
        User user,
        TenantContext tenantContext,
        String clusterName,
        AnalysisType context,
        ActionListener<FieldMappingsView> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (request.indices() == null || request.indices().length == 0) {
            listener.onFailure(new OpenSearchStatusException("Indices are required for field mappings", RestStatus.BAD_REQUEST));
            return;
        }
        if (request.fields() == null || request.fields().length == 0) {
            listener.onFailure(new OpenSearchStatusException("Fields are required for field mappings", RestStatus.BAD_REQUEST));
            return;
        }

        boolean isLocalCluster = CrossClusterConfigUtils.isLocalCluster(clusterName, clusterService);

        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("get_field_mappings", settings, securityHeaders)) {
            if (user != null) {
                securityInjector.inject(user.getName(), user.getRoles());
            }

            RestClient restClient = getRestClient(tenantId);
            if (isLocalCluster) {
                Request restRequest = buildLocalFieldMappingRequest(request);
                restClient.performRequestAsync(restRequest, new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        try {
                            listener.onResponse(parseLocalFieldMappingResponse(response, request.indices()));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                Request restRequest = buildFieldCapsRequest(request, clusterName);
                restClient.performRequestAsync(restRequest, new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        try {
                            FieldCapabilitiesResponse fieldCapsResponse = parseFieldCapsResponse(response);
                            FieldMappingsView mappingsResponse = convertFieldCapsResponse(fieldCapsResponse, request.indices());
                            listener.onResponse(mappingsResponse);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void putPipeline(
        PutPipelineRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("put_pipeline", settings, securityHeaders)) {
            if (user != null) {
                securityInjector.inject(user.getName(), user.getRoles());
            }

            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildPutPipelineRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        listener.onResponse(parseAcknowledgedResponse(response));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void deletePipeline(
        DeletePipelineRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("delete_pipeline", settings, securityHeaders)) {
            if (user != null) {
                securityInjector.inject(user.getName(), user.getRoles());
            }

            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildDeletePipelineRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        listener.onResponse(parseAcknowledgedResponse(response));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void updateSettings(
        UpdateSettingsRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        Settings requestSettings = request.settings();
        if (requestSettings == null) {
            listener.onFailure(new OpenSearchStatusException("Update settings request missing settings", RestStatus.BAD_REQUEST));
            return;
        }

        try (SecurityHeaderInjector securityInjector = new SecurityHeaderInjector("update_settings", settings, securityHeaders)) {
            if (user != null) {
                securityInjector.inject(user.getName(), user.getRoles());
            }

            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildUpdateSettingsRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        listener.onResponse(parseAcknowledgedResponse(response));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private RestClient getRestClient(String tenantId) {
        String endpoint = endpointResolver.resolve(tenantId);
        return RestClientProvider.getRestClient(endpoint);
    }

    private Request buildPutPipelineRequest(PutPipelineRequest request) {
        String encodedPipelineId = URLEncoder.encode(request.getId(), StandardCharsets.UTF_8);
        Request restRequest = new Request("PUT", "/_ingest/pipeline/" + encodedPipelineId);
        ContentType contentType = ContentType
            .parse(request.getMediaType() == null ? MediaTypeRegistry.JSON.mediaType() : request.getMediaType().mediaType());
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(request.getSource()), contentType));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private Request buildDeletePipelineRequest(DeletePipelineRequest request) {
        String encodedPipelineId = URLEncoder.encode(request.getId(), StandardCharsets.UTF_8);
        Request restRequest = new Request("DELETE", "/_ingest/pipeline/" + encodedPipelineId);
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private Request buildUpdateSettingsRestRequest(UpdateSettingsRequest request) throws IOException {
        String indicesPart = (request.indices() == null || request.indices().length == 0) ? "_all" : joinIndices(request.indices());
        Request restRequest = new Request("PUT", "/" + indicesPart + "/_settings");
        applyIndicesOptions(restRequest, request.indicesOptions());
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        request.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(BytesReference.bytes(builder)), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private Request buildLocalFieldMappingRequest(GetFieldMappingsRequest request) {
        String indicesPart = joinIndices(request.indices());
        String fieldsPart = String.join(",", request.fields());
        Request restRequest = new Request("GET", "/" + indicesPart + "/_mapping/field/" + fieldsPart);
        applyIndicesOptions(restRequest, request.indicesOptions());
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private Request buildFieldCapsRequest(GetFieldMappingsRequest request, String clusterName) {
        String indicesPart = joinRemoteIndices(request.indices(), clusterName);
        Request restRequest = new Request("GET", "/" + indicesPart + "/_field_caps");
        restRequest.addParameter("fields", String.join(",", request.fields()));
        restRequest.addParameter("include_unmapped", "true");
        applyIndicesOptions(restRequest, request.indicesOptions());
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void applyIndicesOptions(Request request, IndicesOptions indicesOptions) {
        if (indicesOptions == null) {
            return;
        }
        request.addParameter("ignore_unavailable", Boolean.toString(indicesOptions.ignoreUnavailable()));
        request.addParameter("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));

        List<String> expandWildcards = new ArrayList<>();
        if (indicesOptions.expandWildcardsOpen()) {
            expandWildcards.add("open");
        }
        if (indicesOptions.expandWildcardsClosed()) {
            expandWildcards.add("closed");
        }
        try {
            if (indicesOptions.expandWildcardsHidden()) {
                expandWildcards.add("hidden");
            }
        } catch (NoSuchMethodError e) {
            // ignore for versions that do not expose the hidden flag
        }

        if (!expandWildcards.isEmpty()) {
            request.addParameter("expand_wildcards", String.join(",", expandWildcards));
        }
    }

    private String joinIndices(String[] indices) {
        return Arrays.stream(indices).collect(Collectors.joining(","));
    }

    private String joinRemoteIndices(String[] indices, String clusterName) {
        String sanitizedClusterName = clusterName == null ? "" : clusterName.replace("#local", "");
        String prefix = sanitizedClusterName.isEmpty() ? "" : sanitizedClusterName + ":";
        return Arrays.stream(indices).map(index -> prefix + index).collect(Collectors.joining(","));
    }

    private void applySecurityHeaders(Request request) {
        Map<String, String> headers = securityHeaders.get();
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(optionsBuilder::addHeader);
        }
        request.setOptions(optionsBuilder);
    }

    private AcknowledgedResponse parseAcknowledgedResponse(Response response) throws IOException {
        try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
            return AcknowledgedResponse.fromXContent(parser);
        }
    }

    private FieldMappingsView parseLocalFieldMappingResponse(Response response, String[] requestedIndices) throws IOException {
        Map<String, Map<String, FieldMappingsView.FieldMapping>> mappings = new HashMap<>();
        if (requestedIndices != null) {
            for (String index : requestedIndices) {
                mappings.put(index, new HashMap<>());
            }
        }

        BytesReference bytes = new BytesArray(EntityUtils.toByteArray(response.getEntity()));
        Map<String, Object> parsed = XContentHelper.convertToMap(bytes, true, MediaTypeRegistry.JSON).v2();

        for (Map.Entry<String, Object> indexEntry : parsed.entrySet()) {
            if (!(indexEntry.getValue() instanceof Map)) {
                continue;
            }
            Map<?, ?> indexMap = (Map<?, ?>) indexEntry.getValue();
            Object mappingsObj = indexMap.get("mappings");
            if (!(mappingsObj instanceof Map)) {
                continue;
            }

            Map<String, FieldMappingsView.FieldMapping> perField = mappings.computeIfAbsent(indexEntry.getKey(), k -> new HashMap<>());
            Map<?, ?> fieldsMap = (Map<?, ?>) mappingsObj;
            for (Map.Entry<?, ?> fieldEntry : fieldsMap.entrySet()) {
                if (!(fieldEntry.getValue() instanceof Map)) {
                    continue;
                }
                Map<?, ?> fieldData = (Map<?, ?>) fieldEntry.getValue();
                String fieldName = fieldEntry.getKey().toString();
                Object fullNameObj = fieldData.get("full_name");
                String fullName = fullNameObj == null ? fieldName : fullNameObj.toString();
                Object mappingObj = fieldData.get("mapping");
                if (!(mappingObj instanceof Map)) {
                    continue;
                }
                Map<?, ?> mappingMap = (Map<?, ?>) mappingObj;
                Object fieldMapping = mappingMap.get(fieldName);
                if (fieldMapping == null && !mappingMap.isEmpty()) {
                    fieldMapping = mappingMap.values().iterator().next();
                }
                if (fieldMapping instanceof Map) {
                    Map<String, Object> sourceMap = new HashMap<>();
                    sourceMap.put(fieldName, new HashMap<>((Map<String, Object>) fieldMapping));
                    perField.put(fieldName, new FieldMappingsView.FieldMapping(fullName, sourceMap));
                }
            }
        }

        return new FieldMappingsView(mappings);
    }

    private FieldCapabilitiesResponse parseFieldCapsResponse(Response response) throws IOException {
        try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
            return FieldCapabilitiesResponse.fromXContent(parser);
        }
    }

    private String entityToString(Response response) throws IOException {
        try {
            return EntityUtils.toString(response.getEntity());
        } catch (ParseException e) {
            throw new IOException("Failed to parse response entity", e);
        }
    }

    private FieldMappingsView convertFieldCapsResponse(FieldCapabilitiesResponse fieldCapsResponse, String[] requestedIndices)
        throws IOException {
        String[] indices = (requestedIndices == null || requestedIndices.length == 0) ? fieldCapsResponse.getIndices() : requestedIndices;
        Map<String, Map<String, FieldMappingsView.FieldMapping>> mappings = new HashMap<>();
        if (indices != null) {
            for (String index : indices) {
                mappings.put(index, new HashMap<>());
            }
        }

        Map<String, Map<String, FieldCapabilities>> fieldCaps = fieldCapsResponse.get();
        if (fieldCaps != null) {
            for (Map.Entry<String, Map<String, FieldCapabilities>> fieldEntry : fieldCaps.entrySet()) {
                String fieldName = fieldEntry.getKey();
                Map<String, FieldCapabilities> typeToCapabilities = fieldEntry.getValue();
                if (typeToCapabilities == null) {
                    continue;
                }
                for (FieldCapabilities capabilities : typeToCapabilities.values()) {
                    String fieldType = capabilities.getType();
                    if ("unmapped".equals(fieldType)) {
                        continue;
                    }
                    String[] indicesForField = capabilities.indices();
                    if (indicesForField == null || indicesForField.length == 0) {
                        indicesForField = indices;
                    }
                    if (indicesForField == null) {
                        continue;
                    }

                    Map<String, Object> source = new HashMap<>();
                    Map<String, Object> fieldDefinition = new HashMap<>();
                    fieldDefinition.put(CommonName.TYPE, fieldType);
                    source.put(fieldName, fieldDefinition);
                    FieldMappingsView.FieldMapping metadata = new FieldMappingsView.FieldMapping(fieldName, source);
                    for (String indexName : indicesForField) {
                        mappings.computeIfAbsent(indexName, key -> new HashMap<>()).put(fieldName, metadata);
                    }
                }
            }
        }

        return new FieldMappingsView(mappings);
    }

    private UpdateDataObjectRequest convertUpdateRequest(UpdateRequest request, String tenantId) {
        if (request.doc() == null || request.doc().sourceAsMap() == null) {
            throw new OpenSearchStatusException("Update request missing document", RestStatus.BAD_REQUEST);
        }

        UpdateDataObjectRequest.Builder builder = UpdateDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .dataObject(request.doc().sourceAsMap())
            .retryOnConflict(request.retryOnConflict())
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }
        return builder.build();
    }

    private DeleteDataObjectRequest convertDeleteRequest(DeleteRequest request, String tenantId) {
        DeleteDataObjectRequest.Builder builder = DeleteDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }
        return builder.build();
    }

    @Override
    public void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        if (isConfigIndex(request.index())) {
            configDocumentStore.update(request, tenantContext, listener);
            return;
        }

        if (IndexUtils.shouldUseRestSearch(request.index())) {
            executeRestUpdate(request, tenantId, listener);
            return;
        }

        if (request.doc() == null || request.doc().sourceAsMap() == null) {
            listener.onFailure(new OpenSearchStatusException("Update request missing document", RestStatus.BAD_REQUEST));
            return;
        }

        Map<String, Object> updatedContent = request.doc().sourceAsMap();
        UpdateDataObjectRequest.Builder builder = UpdateDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .dataObject(updatedContent)
            .retryOnConflict(request.retryOnConflict())
            .refreshPolicy(request.getRefreshPolicy())
            .timeout(request.timeout());

        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            builder.ifSeqNo(request.ifSeqNo()).ifPrimaryTerm(request.ifPrimaryTerm());
        }

        sdkClient.updateDataObjectAsync(builder.build()).whenComplete((updateResponse, throwable) -> {
            if (throwable != null) {
                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                return;
            }
            try {
                listener.onResponse(UpdateResponse.fromXContent(updateResponse.parser()));
            } catch (Exception e) {
                listener.onFailure(new OpenSearchStatusException("Failed to parse update response", RestStatus.INTERNAL_SERVER_ERROR));
            }
        });
    }

    private boolean isConfigIndex(String index) {
        return ADCommonName.CONFIG_INDEX.equals(index) || ForecastCommonName.CONFIG_INDEX.equals(index);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, TenantContext tenantContext, ActionListener<MultiSearchResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        boolean validateTenantId = !tenantContext.isSystemWide();
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (validateTenantId && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST));
            return;
        }

        String[] indices = request.requests().stream().map(SearchRequest::indices).flatMap(Arrays::stream).toArray(String[]::new);
        if (IndexUtils.shouldUseRestSearch(indices)) {
            listener.onFailure(new OpenSearchStatusException("Only user indices are supported", RestStatus.BAD_REQUEST));
            return;
        }

        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildMultiSearchRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(MultiSearchResponse.fromXContext(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildMultiSearchRestRequest(MultiSearchRequest request) throws IOException {
        Request restRequest = new Request("POST", "/_msearch");
        // Required when parsing the REST response into MultiSearchResponse (aggregations/suggesters need type info)
        restRequest.addParameter("typed_keys", "true");

        if (request.indicesOptions() != null) {
            applyIndicesOptions(restRequest, request.indicesOptions());
        }

        // Build NDJSON body: each search request has a header line and a body line
        StringBuilder msearchBody = new StringBuilder();
        for (SearchRequest searchRequest : request.requests()) {
            // Header line with index info
            XContentBuilder headerBuilder = XContentFactory.jsonBuilder();
            headerBuilder.startObject();
            if (searchRequest.indices() != null && searchRequest.indices().length > 0) {
                headerBuilder.array("index", searchRequest.indices());
            }
            if (searchRequest.preference() != null) {
                headerBuilder.field("preference", searchRequest.preference());
            }
            if (searchRequest.routing() != null) {
                headerBuilder.field("routing", searchRequest.routing());
            }
            headerBuilder.endObject();
            msearchBody.append(headerBuilder.toString()).append("\n");

            // Body line with search source
            SearchSourceBuilder sourceBuilder = searchRequest.source();
            if (sourceBuilder != null) {
                XContentBuilder bodyBuilder = XContentFactory.jsonBuilder();
                sourceBuilder.toXContent(bodyBuilder, ToXContent.EMPTY_PARAMS);
                msearchBody.append(bodyBuilder.toString()).append("\n");
            } else {
                msearchBody.append("{}\n");
            }
        }

        restRequest.setEntity(new ByteArrayEntity(msearchBody.toString().getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    // ==================== REST-based implementations ====================

    private void executeRestUpdate(UpdateRequest request, String tenantId, ActionListener<UpdateResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildUpdateRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(UpdateResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildUpdateRestRequest(UpdateRequest request) throws IOException {
        String index = URLEncoder.encode(request.index(), StandardCharsets.UTF_8);
        String id = URLEncoder.encode(request.id(), StandardCharsets.UTF_8);
        Request restRequest = new Request("POST", "/" + index + "/_update/" + id);

        if (request.getRefreshPolicy() != RefreshPolicy.NONE) {
            restRequest.addParameter("refresh", request.getRefreshPolicy().getValue());
        }
        if (request.retryOnConflict() > 0) {
            restRequest.addParameter("retry_on_conflict", String.valueOf(request.retryOnConflict()));
        }
        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            restRequest.addParameter("if_seq_no", String.valueOf(request.ifSeqNo()));
        }
        if (request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            restRequest.addParameter("if_primary_term", String.valueOf(request.ifPrimaryTerm()));
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (request.doc() != null) {
            builder.field("doc", request.doc().sourceAsMap());
        }
        if (request.docAsUpsert()) {
            builder.field("doc_as_upsert", true);
        }
        builder.endObject();
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(BytesReference.bytes(builder)), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestUpdateByQuery(UpdateByQueryRequest request, String tenantId, ActionListener<BulkByScrollResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildUpdateByQueryRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(BulkByScrollResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildUpdateByQueryRestRequest(UpdateByQueryRequest request) throws IOException {
        SearchRequest searchRequest = request.getSearchRequest();
        String[] indices = searchRequest.indices();
        String path = (indices == null || indices.length == 0) ? "/_update_by_query" : "/" + joinIndices(indices) + "/_update_by_query";
        Request restRequest = new Request("POST", path);

        applyIndicesOptions(restRequest, searchRequest.indicesOptions());
        if (request.isRefresh()) {
            restRequest.addParameter("refresh", "true");
        }
        if (request.getMaxRetries() > 0) {
            restRequest.addParameter("max_retries", String.valueOf(request.getMaxRetries()));
        }
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES) {
            restRequest.addParameter("max_docs", String.valueOf(request.getMaxDocs()));
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        if (sourceBuilder != null && sourceBuilder.query() != null) {
            builder.field("query", sourceBuilder.query());
        }
        Script script = request.getScript();
        if (script != null) {
            builder.field("script", script);
        }
        builder.endObject();
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(BytesReference.bytes(builder)), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestDelete(DeleteRequest request, String tenantId, ActionListener<DeleteResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildDeleteRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(DeleteResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildDeleteRestRequest(DeleteRequest request) {
        String index = URLEncoder.encode(request.index(), StandardCharsets.UTF_8);
        String id = URLEncoder.encode(request.id(), StandardCharsets.UTF_8);
        Request restRequest = new Request("DELETE", "/" + index + "/_doc/" + id);

        if (request.getRefreshPolicy() != RefreshPolicy.NONE) {
            restRequest.addParameter("refresh", request.getRefreshPolicy().getValue());
        }
        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            restRequest.addParameter("if_seq_no", String.valueOf(request.ifSeqNo()));
        }
        if (request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            restRequest.addParameter("if_primary_term", String.valueOf(request.ifPrimaryTerm()));
        }
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestDeleteByQuery(DeleteByQueryRequest request, String tenantId, ActionListener<BulkByScrollResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildDeleteByQueryRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(BulkByScrollResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildDeleteByQueryRestRequest(DeleteByQueryRequest request) throws IOException {
        SearchRequest searchRequest = request.getSearchRequest();
        String[] indices = searchRequest.indices();
        String path = (indices == null || indices.length == 0) ? "/_delete_by_query" : "/" + joinIndices(indices) + "/_delete_by_query";
        Request restRequest = new Request("POST", path);

        applyIndicesOptions(restRequest, searchRequest.indicesOptions());
        if (request.isRefresh()) {
            restRequest.addParameter("refresh", "true");
        }
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES) {
            restRequest.addParameter("max_docs", String.valueOf(request.getMaxDocs()));
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        if (sourceBuilder != null && sourceBuilder.query() != null) {
            builder.field("query", sourceBuilder.query());
        }
        builder.endObject();
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(BytesReference.bytes(builder)), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestIndex(IndexRequest request, String tenantId, ActionListener<IndexResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildIndexRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(IndexResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildIndexRestRequest(IndexRequest request) {
        String index = URLEncoder.encode(request.index(), StandardCharsets.UTF_8);
        String method;
        String path;
        if (request.id() != null && !request.id().isEmpty()) {
            String id = URLEncoder.encode(request.id(), StandardCharsets.UTF_8);
            method = request.opType() == IndexRequest.OpType.CREATE ? "PUT" : "PUT";
            path = "/" + index + "/_doc/" + id;
            if (request.opType() == IndexRequest.OpType.CREATE) {
                path = "/" + index + "/_create/" + id;
            }
        } else {
            method = "POST";
            path = "/" + index + "/_doc";
        }
        Request restRequest = new Request(method, path);

        if (request.getRefreshPolicy() != RefreshPolicy.NONE) {
            restRequest.addParameter("refresh", request.getRefreshPolicy().getValue());
        }
        if (request.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            restRequest.addParameter("if_seq_no", String.valueOf(request.ifSeqNo()));
        }
        if (request.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            restRequest.addParameter("if_primary_term", String.valueOf(request.ifPrimaryTerm()));
        }
        restRequest.setEntity(new ByteArrayEntity(BytesReference.toBytes(request.source()), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestBulk(BulkRequest request, String tenantId, ActionListener<BulkResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildBulkRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(BulkResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildBulkRestRequest(BulkRequest request) throws IOException {
        Request restRequest = new Request("POST", "/_bulk");

        if (request.getRefreshPolicy() != RefreshPolicy.NONE) {
            restRequest.addParameter("refresh", request.getRefreshPolicy().getValue());
        }

        StringBuilder bulkBody = new StringBuilder();
        for (DocWriteRequest<?> docRequest : request.requests()) {
            if (docRequest instanceof IndexRequest) {
                IndexRequest indexReq = (IndexRequest) docRequest;
                bulkBody.append("{\"index\":{\"_index\":\"").append(indexReq.index()).append("\"");
                if (indexReq.id() != null) {
                    bulkBody.append(",\"_id\":\"").append(indexReq.id()).append("\"");
                }
                bulkBody.append("}}\n");
                bulkBody.append(indexReq.source().utf8ToString()).append("\n");
            } else if (docRequest instanceof UpdateRequest) {
                UpdateRequest updateReq = (UpdateRequest) docRequest;
                bulkBody
                    .append("{\"update\":{\"_index\":\"")
                    .append(updateReq.index())
                    .append("\",\"_id\":\"")
                    .append(updateReq.id())
                    .append("\"");
                if (updateReq.retryOnConflict() > 0) {
                    bulkBody.append(",\"retry_on_conflict\":").append(updateReq.retryOnConflict());
                }
                bulkBody.append("}}\n");
                if (updateReq.doc() != null) {
                    XContentBuilder docBuilder = XContentFactory.jsonBuilder();
                    docBuilder.startObject().field("doc", updateReq.doc().sourceAsMap()).endObject();
                    bulkBody.append(docBuilder.toString()).append("\n");
                }
            } else if (docRequest instanceof DeleteRequest) {
                DeleteRequest deleteReq = (DeleteRequest) docRequest;
                bulkBody
                    .append("{\"delete\":{\"_index\":\"")
                    .append(deleteReq.index())
                    .append("\",\"_id\":\"")
                    .append(deleteReq.id())
                    .append("\"}}\n");
            }
        }

        restRequest.setEntity(new ByteArrayEntity(bulkBody.toString().getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON));
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private void executeRestGet(GetRequest request, String tenantId, ActionListener<GetResponse> listener) {
        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildGetRestRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try (XContentParser parser = SdkClientUtils.createParser(entityToString(response))) {
                        listener.onResponse(GetResponse.fromXContent(parser));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildGetRestRequest(GetRequest request) {
        String index = URLEncoder.encode(request.index(), StandardCharsets.UTF_8);
        String id = URLEncoder.encode(request.id(), StandardCharsets.UTF_8);
        Request restRequest = new Request("GET", "/" + index + "/_doc/" + id);

        if (request.fetchSourceContext() != null) {
            if (!request.fetchSourceContext().fetchSource()) {
                restRequest.addParameter("_source", "false");
            } else if (request.fetchSourceContext().includes() != null && request.fetchSourceContext().includes().length > 0) {
                restRequest.addParameter("_source_includes", String.join(",", request.fetchSourceContext().includes()));
            }
            if (request.fetchSourceContext().excludes() != null && request.fetchSourceContext().excludes().length > 0) {
                restRequest.addParameter("_source_excludes", String.join(",", request.fetchSourceContext().excludes()));
            }
        }
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private NodeInfo buildNodeInfo(DiscoveryNode node) {
        PluginInfo pluginInfo = new PluginInfo(
            ADCommonName.AD_PLUGIN_NAME,
            "",
            Version.CURRENT.toString(),
            Version.CURRENT,
            System.getProperty("java.version"),
            "",
            "",
            Collections.emptyList(),
            false
        );
        PluginsAndModules plugins = new PluginsAndModules(Collections.singletonList(pluginInfo), Collections.emptyList());
        return new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            plugins,
            null,
            null,
            null,
            null
        );
    }

    @Override
    public void concreteIndexNames(
        IndicesOptions indicesOptions,
        String[] indexExpressions,
        TenantContext tenantContext,
        ActionListener<String[]> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();

        try {
            RestClient restClient = getRestClient(tenantId);
            Request restRequest = buildResolveIndexRequest(indexExpressions, indicesOptions);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        String[] concreteIndices = parseResolveIndexResponse(response);
                        listener.onResponse(concreteIndices);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Request buildResolveIndexRequest(String[] indexExpressions, IndicesOptions indicesOptions) {
        String indicesPart = Arrays.stream(indexExpressions).collect(Collectors.joining(","));
        String encodedIndices = URLEncoder.encode(indicesPart, StandardCharsets.UTF_8);
        Request restRequest = new Request("GET", "/_resolve/index/" + encodedIndices);
        applyIndicesOptions(restRequest, indicesOptions);
        applySecurityHeaders(restRequest);
        return restRequest;
    }

    private String[] parseResolveIndexResponse(Response response) throws IOException {
        BytesReference bytes = new BytesArray(EntityUtils.toByteArray(response.getEntity()));
        Map<String, Object> parsed = XContentHelper.convertToMap(bytes, true, MediaTypeRegistry.JSON).v2();

        List<String> concreteIndices = new ArrayList<>();

        // Extract index names from "indices" array
        Object indicesObj = parsed.get("indices");
        if (indicesObj instanceof List) {
            for (Object indexEntry : (List<?>) indicesObj) {
                if (indexEntry instanceof Map) {
                    Object nameObj = ((Map<?, ?>) indexEntry).get("name");
                    if (nameObj != null) {
                        concreteIndices.add(nameObj.toString());
                    }
                }
            }
        }

        return concreteIndices.toArray(new String[0]);
    }
}
