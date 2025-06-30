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

package org.opensearch.ad.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.BaseEntityProfileTransportAction;
import org.opensearch.timeseries.transport.DeleteModelNodeResponse;
import org.opensearch.timeseries.transport.DeleteModelRequest;
import org.opensearch.timeseries.transport.DeleteModelResponse;
import org.opensearch.timeseries.transport.EntityProfileRequest;
import org.opensearch.timeseries.transport.EntityProfileResponse;
import org.opensearch.timeseries.transport.EntityResultRequest;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SDKNodeFilter;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public abstract class HttpNodeCommunicator implements NodeCommunicator {
    private static final String NODES_KEY = "nodes";
    protected final SDKNodeFilter nodeFilter;
    protected final String restPath;
    protected final String internalRestPath;
    protected final String clusterName;
    protected final HashRing hashRing;
    private final String internalApiSharedSecret;

    public HttpNodeCommunicator(
        String restPath,
        String internalRestPath,
        String clusterName,
        HashRing hashRing,
        String internalApiSharedSecret,
        Settings settings
    ) {
        this.restPath = Objects.requireNonNull(restPath, "restPath must not be null");
        this.internalRestPath = Objects.requireNonNull(internalRestPath, "internalRestPath must not be null");
        this.nodeFilter = new SDKNodeFilter(settings);
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName must not be null");
        this.hashRing = Objects.requireNonNull(hashRing, "hashRing must not be null");
        this.internalApiSharedSecret = Objects.requireNonNull(internalApiSharedSecret, "internalApiSharedSecret must not be null");
    }

    @Override
    public void profile(ProfileRequest request, ActionListener<ProfileResponse> listener) {
        DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();
        if (nodes == null || nodes.length == 0) {
            listener.onResponse(new ProfileResponse(new ClusterName(clusterName), Collections.emptyList(), Collections.emptyList()));
            return;
        }

        List<ProfileNodeResponse> responses = Collections.synchronizedList(new ArrayList<>());
        List<FailedNodeException> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger pending = new AtomicInteger(nodes.length);

        for (DiscoveryNode node : nodes) {
            String endpoint = getNodeEndpoint(node);
            if (endpoint == null) {
                failures.add(new FailedNodeException(node == null ? "" : node.getId(), "Missing node endpoint", null));
                if (pending.decrementAndGet() == 0) {
                    listener.onResponse(new ProfileResponse(new ClusterName(clusterName), responses, failures));
                }
                continue;
            }
            RestClient restClient = RestClientProvider.getRestClient(endpoint);
            Request restRequest = buildProfileRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        responses.add(parseProfileNodeResponse(node, response));
                    } catch (Exception e) {
                        failures.add(new FailedNodeException(node.getId(), "Failed to parse profile response", e));
                    }
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new ProfileResponse(new ClusterName(clusterName), responses, failures));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(new FailedNodeException(node.getId(), "Failed to call profile API", e));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new ProfileResponse(new ClusterName(clusterName), responses, failures));
                    }
                }
            });
        }
    }

    @Override
    public void entityProfile(EntityProfileRequest request, ActionListener<EntityProfileResponse> listener) {
        if (request == null) {
            listener.onFailure(new IllegalArgumentException("Entity profile request is missing"));
            return;
        }
        Entity entity = request.getEntityValue();
        if (entity == null) {
            listener.onFailure(new IllegalArgumentException("Entity value is missing"));
            return;
        }
        // we cannot depend on the hash ring routing in BaseEntityProfileTransportAction.doExecute to redirect to the correct node
        // because multitenant environment only has single-node cluster.
        DiscoveryNode node = hashRing.getOwningNodeWithSameLocalVersionForRealtime(entity.toString()).orElse(null);
        if (node == null) {
            listener.onFailure(new TimeSeriesException(request.getConfigID(), BaseEntityProfileTransportAction.NO_NODE_FOUND_MSG));
            return;
        }
        String endpoint = getNodeEndpoint(node);
        if (endpoint == null) {
            listener.onFailure(new FailedNodeException(node.getId(), "Missing node endpoint", null));
            return;
        }
        Request restRequest;
        try {
            restRequest = buildEntityProfileRequest(request);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }
        RestClient restClient = RestClientProvider.getRestClient(endpoint);
        restClient.performRequestAsync(restRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    listener.onResponse(parseEntityProfileResponse(response));
                } catch (Exception e) {
                    listener.onFailure(new FailedNodeException(node.getId(), "Failed to parse entity profile response", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new FailedNodeException(node.getId(), "Failed to call entity profile API", e));
            }
        });
    }

    @Override
    public void entityResult(
        DiscoveryNode node,
        EntityResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Entity result request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Entity result node is missing"));
            return;
        }
        String endpoint = getNodeEndpoint(node);
        if (endpoint == null) {
            responseHandler.handleException(new TransportException("Missing node endpoint"));
            return;
        }
        Request restRequest;
        try {
            restRequest = buildEntityResultRequest(request);
        } catch (IOException e) {
            responseHandler.handleException(new TransportException("Failed to build entity result request", e));
            return;
        }
        RestClient restClient = RestClientProvider.getRestClient(endpoint);
        restClient.performRequestAsync(restRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    responseHandler.handleResponse(parseAcknowledgedResponse(response));
                } catch (Exception e) {
                    responseHandler.handleException(new TransportException("Failed to parse entity result response", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                responseHandler.handleException(new TransportException("Failed to call entity result API", e));
            }
        });
    }

    @Override
    public void singleStreamResult(
        DiscoveryNode node,
        SingleStreamResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Single stream result request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Single stream result node is missing"));
            return;
        }
        String endpoint = getNodeEndpoint(node);
        if (endpoint == null) {
            responseHandler.handleException(new TransportException("Missing node endpoint"));
            return;
        }
        Request restRequest;
        try {
            restRequest = buildSingleStreamResultRequest(request);
        } catch (IOException e) {
            responseHandler.handleException(new TransportException("Failed to build single stream result request", e));
            return;
        }
        RestClient restClient = RestClientProvider.getRestClient(endpoint);
        restClient.performRequestAsync(restRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    responseHandler.handleResponse(parseAcknowledgedResponse(response));
                } catch (Exception e) {
                    responseHandler.handleException(new TransportException("Failed to parse single stream result response", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                responseHandler.handleException(new TransportException("Failed to call single stream result API", e));
            }
        });
    }

    @Override
    public void deleteModel(DeleteModelRequest request, ActionListener<DeleteModelResponse> listener) {
        if (request == null) {
            listener.onFailure(new IllegalArgumentException("Delete model request is missing"));
            return;
        }
        String configId = request.getAdID();
        if (Strings.isNullOrEmpty(configId)) {
            listener.onFailure(new IllegalArgumentException("Config ID is missing"));
            return;
        }
        DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();
        if (nodes == null || nodes.length == 0) {
            listener.onResponse(new DeleteModelResponse(new ClusterName(clusterName), Collections.emptyList(), Collections.emptyList()));
            return;
        }

        List<DeleteModelNodeResponse> responses = Collections.synchronizedList(new ArrayList<>());
        List<FailedNodeException> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger pending = new AtomicInteger(nodes.length);

        for (DiscoveryNode node : nodes) {
            String endpoint = getNodeEndpoint(node);
            if (endpoint == null) {
                failures.add(new FailedNodeException(node == null ? "" : node.getId(), "Missing node endpoint", null));
                if (pending.decrementAndGet() == 0) {
                    listener.onResponse(new DeleteModelResponse(new ClusterName(clusterName), responses, failures));
                }
                continue;
            }

            Request restRequest = buildDeleteModelRequest(configId, request.getTenantId());
            RestClient restClient = RestClientProvider.getRestClient(endpoint);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    responses.add(new DeleteModelNodeResponse(node));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new DeleteModelResponse(new ClusterName(clusterName), responses, failures));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(new FailedNodeException(node.getId(), "Failed to call delete model API", e));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new DeleteModelResponse(new ClusterName(clusterName), responses, failures));
                    }
                }
            });
        }
    }

    @Override
    public void stat(StatsRequest request, ActionListener<StatsNodesResponse> listener) {
        if (request == null) {
            listener.onFailure(new IllegalArgumentException("Stats request is missing"));
            return;
        }
        DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();
        if (nodes == null || nodes.length == 0) {
            listener.onResponse(new StatsNodesResponse(new ClusterName(clusterName), Collections.emptyList(), Collections.emptyList()));
            return;
        }

        List<DiscoveryNode> targetNodes = resolveTargetNodes(nodes, request);
        if (targetNodes.isEmpty()) {
            listener.onResponse(new StatsNodesResponse(new ClusterName(clusterName), Collections.emptyList(), Collections.emptyList()));
            return;
        }

        List<StatsNodeResponse> responses = Collections.synchronizedList(new ArrayList<>());
        List<FailedNodeException> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger pending = new AtomicInteger(targetNodes.size());

        for (DiscoveryNode node : targetNodes) {
            String endpoint = getNodeEndpoint(node);
            if (endpoint == null) {
                failures.add(new FailedNodeException(node == null ? "" : node.getId(), "Missing node endpoint", null));
                if (pending.decrementAndGet() == 0) {
                    listener.onResponse(new StatsNodesResponse(new ClusterName(clusterName), responses, failures));
                }
                continue;
            }
            RestClient restClient = RestClientProvider.getRestClient(endpoint);
            Request restRequest = buildStatsRequest(request);
            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        responses.add(parseStatsNodeResponse(node, response));
                    } catch (Exception e) {
                        failures.add(new FailedNodeException(node.getId(), "Failed to parse stats response", e));
                    }
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new StatsNodesResponse(new ClusterName(clusterName), responses, failures));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(new FailedNodeException(node.getId(), "Failed to call stats API", e));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new StatsNodesResponse(new ClusterName(clusterName), responses, failures));
                    }
                }
            });
        }
    }

    private Request buildProfileRequest(ProfileRequest request) {
        String typeStr = convertProfilesToTypeStr(request.getProfilesToBeRetrieved());
        Request restRequest = new Request("POST", buildProfilePath(request.getConfigId(), typeStr));
        applyInternalRequestHeaders(restRequest, request.getTenantId());
        return restRequest;
    }

    private Request buildEntityProfileRequest(EntityProfileRequest request) throws IOException {
        String typeStr = convertEntityProfilesToTypeStr(request.getProfilesToCollect());
        Request restRequest = new Request("POST", buildEntityProfilePath(request.getConfigID(), typeStr));
        String body = buildEntityProfileBody(request.getEntityValue(), request.getTenantId());
        if (body != null) {
            restRequest.setJsonEntity(body);
        }
        if (request.getTenantId() != null) {
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(CommonName.TENANT_ID_HEADER, request.getTenantId());
            restRequest.setOptions(optionsBuilder);
        }
        return restRequest;
    }

    private String convertProfilesToTypeStr(Set<ProfileName> profiles) {
        if (profiles == null || profiles.isEmpty()) {
            return null;
        }
        return profiles.stream().map(ProfileName::getName).collect(Collectors.joining(","));
    }

    private String convertEntityProfilesToTypeStr(Set<EntityProfileName> profiles) {
        if (profiles == null || profiles.isEmpty()) {
            return null;
        }
        return profiles.stream().map(EntityProfileName::getName).collect(Collectors.joining(","));
    }

    private String buildEntityProfilePath(String configId, String typeStr) {
        StringBuilder path = new StringBuilder();
        path.append(restPath).append("/").append(configId).append("/").append(RestHandlerUtils.ENTITY_PROFILE);
        if (!Strings.isNullOrEmpty(typeStr)) {
            path.append("/").append(typeStr);
        }
        return path.toString();
    }

    protected String buildEntityResultPath(String configId) {
        return new StringBuilder()
            .append(internalRestPath)
            .append("/")
            .append(configId)
            .append("/")
            .append(RestHandlerUtils.ENTITY_RESULT)
            .toString();
    }

    protected String buildSingleStreamResultPath(String configId) {
        return new StringBuilder()
            .append(internalRestPath)
            .append("/")
            .append(configId)
            .append("/")
            .append(RestHandlerUtils.SINGLE_STREAM_RESULT)
            .toString();
    }

    private Request buildDeleteModelRequest(String configId, String tenantId) {
        Request restRequest = new Request("POST", buildDeleteModelPath(configId));
        applyInternalRequestHeaders(restRequest, tenantId);
        return restRequest;
    }

    private Request buildStatsRequest(StatsRequest request) {
        // This REST client is already created from the selected node endpoint, so this request is per-node without a nodeId parameter.
        Request restRequest = new Request("GET", buildStatsPath());
        Set<String> stats = request.getStatsToBeRetrieved();
        if (stats != null && !stats.isEmpty()) {
            restRequest.addParameter("stat", String.join(",", stats));
        }
        applyInternalRequestHeaders(restRequest, request.getTenantId());
        return restRequest;
    }

    private String buildDeleteModelPath(String configId) {
        return new StringBuilder()
            .append(internalRestPath)
            .append("/")
            .append(configId)
            .append("/")
            .append(RestHandlerUtils.DELETE_MODEL)
            .toString();
    }

    private String buildStatsPath() {
        return new StringBuilder().append(internalRestPath).append("/").append(RestHandlerUtils.STATS_NODES).toString();
    }

    private Request buildEntityResultRequest(EntityResultRequest request) throws IOException {
        String path = buildEntityResultPath(request.getConfigId());
        Request restRequest = new Request("POST", path);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            restRequest.setJsonEntity(BytesReference.bytes(builder).utf8ToString());
        }
        applyInternalRequestHeaders(restRequest, request.getTenantId());
        return restRequest;
    }

    private Request buildSingleStreamResultRequest(SingleStreamResultRequest request) throws IOException {
        String path = buildSingleStreamResultPath(request.getConfigId());
        Request restRequest = new Request("POST", path);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request.toXContent(builder, ToXContent.EMPTY_PARAMS);
            restRequest.setJsonEntity(BytesReference.bytes(builder).utf8ToString());
        }
        applyInternalRequestHeaders(restRequest, request.getTenantId());
        return restRequest;
    }

    protected void applyInternalRequestHeaders(Request restRequest, String tenantId) {
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader(CommonName.INTERNAL_API_TOKEN_HEADER, internalApiSharedSecret);
        if (tenantId != null) {
            optionsBuilder.addHeader(CommonName.TENANT_ID_HEADER, tenantId);
        }
        restRequest.setOptions(optionsBuilder);
    }

    private String buildEntityProfileBody(Entity entity, String tenantId) throws IOException {
        if (entity == null) {
            return null;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(CommonName.ENTITY_KEY, entity);
            if (tenantId != null) {
                builder.field(CommonName.TENANT_ID_FIELD, tenantId);
            }
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private ProfileNodeResponse parseProfileNodeResponse(DiscoveryNode node, Response response) throws IOException, ParseException {
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> map = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);
        return parseDetectorProfileToProfileNodeResponse(node, map);
    }

    @SuppressWarnings("unchecked")
    private StatsNodeResponse parseStatsNodeResponse(DiscoveryNode node, Response response) throws IOException, ParseException {
        String responseBody = EntityUtils.toString(response.getEntity());
        if (responseBody == null || responseBody.isEmpty()) {
            return new StatsNodeResponse(node, Collections.emptyMap());
        }
        Map<String, Object> map = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);
        Object nodesObj = map.get(NODES_KEY);
        if (!(nodesObj instanceof Map)) {
            return new StatsNodeResponse(node, Collections.emptyMap());
        }
        Map<String, Object> nodesMap = (Map<String, Object>) nodesObj;
        Object nodeStatsObj = nodesMap.get(node.getId());
        if (!(nodeStatsObj instanceof Map) && !nodesMap.isEmpty()) {
            nodeStatsObj = nodesMap.values().iterator().next();
        }
        if (!(nodeStatsObj instanceof Map)) {
            return new StatsNodeResponse(node, Collections.emptyMap());
        }
        return new StatsNodeResponse(node, (Map<String, Object>) nodeStatsObj);
    }

    private List<DiscoveryNode> resolveTargetNodes(DiscoveryNode[] nodes, StatsRequest request) {
        String[] requestedNodeIds = request.nodesIds();
        if (requestedNodeIds == null || requestedNodeIds.length == 0) {
            return Arrays.asList(nodes);
        }
        Set<String> requested = new HashSet<>(Arrays.asList(requestedNodeIds));
        if (requested.contains("_all")) {
            return Arrays.asList(nodes);
        }
        return Arrays
            .stream(nodes)
            .filter(node -> requested.contains(node.getId()) || requested.contains(node.getName()))
            .collect(Collectors.toList());
    }

    private EntityProfileResponse parseEntityProfileResponse(Response response) throws IOException, ParseException {
        String responseBody = EntityUtils.toString(response.getEntity());
        if (responseBody == null || responseBody.isEmpty()) {
            return new EntityProfileResponse(null, -1L, -1L, null);
        }
        Map<String, Object> map = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);
        return parseEntityProfileResponse(map);
    }

    private EntityProfileResponse parseEntityProfileResponse(Map<String, Object> map) throws IOException {
        Boolean active = asBoolean(map.get(EntityProfileResponse.ACTIVE));
        Long lastActive = asLong(map.get(EntityProfileResponse.LAST_ACTIVE_TS));
        Long totalUpdates = asLong(map.get(EntityProfileResponse.TOTAL_UPDATES));
        ModelProfileOnNode modelProfile = parseModelProfileOnNode(map.get(CommonName.MODEL));
        return new EntityProfileResponse(
            active,
            lastActive != null ? lastActive : -1L,
            totalUpdates != null ? totalUpdates : -1L,
            modelProfile
        );
    }

    private AcknowledgedResponse parseAcknowledgedResponse(Response response) throws IOException, ParseException {
        String responseBody = EntityUtils.toString(response.getEntity());
        if (responseBody == null || responseBody.isEmpty()) {
            return new AcknowledgedResponse(true);
        }
        try (XContentParser parser = SdkClientUtils.createParser(responseBody)) {
            return AcknowledgedResponse.fromXContent(parser);
        }
    }

    @SuppressWarnings("unchecked")
    private ModelProfileOnNode parseModelProfileOnNode(Object value) throws IOException {
        if (!(value instanceof Map)) {
            return null;
        }
        Map<String, Object> map = (Map<String, Object>) value;
        String nodeId = asString(map.get(ModelProfileOnNode.NODE_ID));
        ModelProfile profile = parseModelProfile(map, null);
        if (profile == null || nodeId == null) {
            return null;
        }
        return new ModelProfileOnNode(nodeId, profile);
    }

    private ProfileNodeResponse parseDetectorProfileToProfileNodeResponse(DiscoveryNode node, Map<String, Object> map) throws IOException {
        String coordinatingNode = asString(map.get(CommonName.COORDINATING_NODE));
        Long activeEntities = asLong(map.get(CommonName.ACTIVE_ENTITIES));
        Long totalUpdates = asLong(map.get(CommonName.TOTAL_UPDATES));
        Long modelCount = asLong(map.get(CommonName.MODEL_COUNT));
        ParsedModelProfiles parsedModels = parseModelProfiles(map.get(CommonName.MODELS));
        List<ModelProfile> modelProfiles = parsedModels.modelProfiles;
        Map<String, Long> modelSize = parsedModels.modelSize;
        long resolvedModelCount = modelCount != null ? modelCount : (modelProfiles == null ? 0L : modelProfiles.size());
        boolean isCoordinatingNode = coordinatingNode != null && !coordinatingNode.isEmpty();

        return new ProfileNodeResponse(
            node,
            modelSize,
            activeEntities != null ? activeEntities : 0L,
            totalUpdates != null ? totalUpdates : 0L,
            modelProfiles,
            resolvedModelCount,
            isCoordinatingNode
        );
    }

    protected String getNodeEndpoint(DiscoveryNode node) {
        if (node == null || node.getAddress() == null) {
            return null;
        }
        String address = node.getAddress().toString();
        if (Strings.isNullOrEmpty(address)) {
            return null;
        }
        if (address.startsWith("/")) {
            address = address.substring(1);
        }
        int lastSlash = address.lastIndexOf('/');
        if (lastSlash >= 0) {
            address = address.substring(lastSlash + 1);
        }
        return address;
    }

    @SuppressWarnings("unchecked")
    private ParsedModelProfiles parseModelProfiles(Object value) throws IOException {
        if (!(value instanceof List)) {
            return new ParsedModelProfiles(null, null);
        }
        List<?> models = (List<?>) value;
        List<ModelProfile> profiles = new ArrayList<>();
        Map<String, Long> modelSize = new HashMap<>();
        for (Object modelValue : models) {
            if (modelValue instanceof Map) {
                ModelProfile profile = parseModelProfile((Map<String, Object>) modelValue, modelSize);
                if (profile != null) {
                    profiles.add(profile);
                }
            }
        }
        List<ModelProfile> resolvedProfiles = profiles.isEmpty() ? null : profiles;
        Map<String, Long> resolvedModelSize = modelSize.isEmpty() ? null : modelSize;
        return new ParsedModelProfiles(resolvedProfiles, resolvedModelSize);
    }

    private ModelProfile parseModelProfile(Map<String, Object> map, Map<String, Long> modelSize) throws IOException {
        String modelId = asString(map.get(CommonName.MODEL_ID_FIELD));
        if (modelId == null) {
            return null;
        }
        Object entityValue = map.get(CommonName.ENTITY_KEY);
        Entity entity = null;
        if (entityValue != null) {
            entity = Entity.fromJsonArray(entityValue);
        }
        Long modelSizeInBytes = asLong(map.get(CommonName.MODEL_SIZE_IN_BYTES));
        long modelSizeValue = modelSizeInBytes == null ? 0L : modelSizeInBytes;
        if (modelSizeInBytes != null && modelSize != null) {
            modelSize.put(modelId, modelSizeValue);
        }
        return new ModelProfile(modelId, entity, modelSizeValue);
    }

    private String asString(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    private Long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Boolean asBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return null;
    }

    private static class ParsedModelProfiles {
        private final List<ModelProfile> modelProfiles;
        private final Map<String, Long> modelSize;

        private ParsedModelProfiles(List<ModelProfile> modelProfiles, Map<String, Long> modelSize) {
            this.modelProfiles = modelProfiles;
            this.modelSize = modelSize;
        }
    }

    protected abstract String buildProfilePath(String configId, String typeStr);
}
