/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.opensearch.timeseries.client.DataPlaneClientFactoryContext.preserveCurrentRequestContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.ExceptionUtil;

/**
 * Config store backed by the remote metadata SDK client.
 */
public class RemoteMetadataConfigDocumentStore implements ConfigDocumentStore {
    private final SdkClient sdkClient;

    public RemoteMetadataConfigDocumentStore(SdkClient sdkClient) {
        this.sdkClient = Objects.requireNonNull(sdkClient, "sdkClient must not be null");
    }

    @Override
    public void search(SearchRequest request, TenantContext tenantContext, ActionListener<SearchResponse> listener) {
        try {
            String tenantId = resolveTenantId(tenantContext);
            SearchSourceBuilder sourceBuilder = request.source();
            if (sourceBuilder == null) {
                sourceBuilder = new SearchSourceBuilder();
                request.source(sourceBuilder);
            }
            SearchDataObjectRequest sdkRequest = SearchDataObjectRequest
                .builder()
                .indices(request.indices())
                .tenantId(tenantId)
                .searchSourceBuilder(sourceBuilder)
                .build();

            sdkClient.searchDataObjectAsync(sdkRequest).whenComplete(preserveCurrentRequestContext((response, throwable) -> {
                if (throwable != null) {
                    if (ExceptionUtil.isIndexNotFoundInMessage(throwable)) {
                        listener.onResponse(SdkSearchResponseUtils.emptySearchResponse());
                        return;
                    }
                    listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                    return;
                }

                try {
                    listener.onResponse(parseSearchResponse(response));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener) {
        try {
            String tenantId = resolveTenantId(tenantContext);
            GetDataObjectRequest sdkRequest = GetDataObjectRequest
                .builder()
                .index(request.index())
                .id(request.id())
                .tenantId(tenantId)
                .fetchSourceContext(request.fetchSourceContext())
                .build();

            sdkClient.getDataObjectAsync(sdkRequest).whenComplete(preserveCurrentRequestContext((response, throwable) -> {
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
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener) {
        try {
            String tenantId = resolveTenantId(tenantContext);
            PutDataObjectRequest sdkRequest = convertIndexRequest(request, tenantId);
            sdkClient.putDataObjectAsync(sdkRequest).whenComplete(preserveCurrentRequestContext((putResponse, throwable) -> {
                if (throwable != null) {
                    listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                    return;
                }

                try {
                    listener.onResponse(IndexResponse.fromXContent(putResponse.parser()));
                } catch (Exception e) {
                    listener.onFailure(new OpenSearchStatusException("Failed to parse index response", RestStatus.INTERNAL_SERVER_ERROR));
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener) {
        try {
            String tenantId = resolveTenantId(tenantContext);
            UpdateDataObjectRequest sdkRequest = convertUpdateRequest(request, tenantId);
            sdkClient.updateDataObjectAsync(sdkRequest).whenComplete(preserveCurrentRequestContext((updateResponse, throwable) -> {
                if (throwable != null) {
                    listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                    return;
                }
                try {
                    listener.onResponse(UpdateResponse.fromXContent(updateResponse.parser()));
                } catch (Exception e) {
                    listener.onFailure(new OpenSearchStatusException("Failed to parse update response", RestStatus.INTERNAL_SERVER_ERROR));
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener) {
        try {
            String tenantId = resolveTenantId(tenantContext);
            DeleteDataObjectRequest sdkRequest = convertDeleteRequest(request, tenantId);
            sdkClient.deleteDataObjectAsync(sdkRequest).whenComplete(preserveCurrentRequestContext((deleteResponse, throwable) -> {
                if (throwable != null) {
                    listener.onFailure(SdkClientUtils.unwrapAndConvertToException(throwable));
                    return;
                }

                try {
                    listener.onResponse(DeleteResponse.fromXContent(deleteResponse.parser()));
                } catch (Exception e) {
                    listener.onFailure(new OpenSearchStatusException("Failed to parse delete response", RestStatus.INTERNAL_SERVER_ERROR));
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String resolveTenantId(TenantContext tenantContext) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.isSystemWide() ? null : tenantContext.getTenantId();
        if (!tenantContext.isSystemWide() && tenantId == null) {
            throw new OpenSearchStatusException("Tenant ID is required", RestStatus.BAD_REQUEST);
        }
        return tenantId;
    }

    private PutDataObjectRequest convertIndexRequest(IndexRequest request, String tenantId) {
        Map<String, Object> sourceAsMap = request.sourceAsMap();
        if (sourceAsMap == null) {
            throw new OpenSearchStatusException("Index request missing document", RestStatus.BAD_REQUEST);
        }
        sourceAsMap = addTenantIdField(sourceAsMap, tenantId);

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

    private UpdateDataObjectRequest convertUpdateRequest(UpdateRequest request, String tenantId) {
        if (request.doc() == null || request.doc().sourceAsMap() == null) {
            throw new OpenSearchStatusException("Update request missing document", RestStatus.BAD_REQUEST);
        }
        Map<String, Object> sourceAsMap = addTenantIdField(request.doc().sourceAsMap(), tenantId);

        UpdateDataObjectRequest.Builder builder = UpdateDataObjectRequest
            .builder()
            .index(request.index())
            .id(request.id())
            .tenantId(tenantId)
            .dataObject(sourceAsMap)
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

    private SearchResponse parseSearchResponse(SearchDataObjectResponse response) {
        if (response == null) {
            throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            SearchResponse searchResponse = response.searchResponse();
            if (searchResponse == null) {
                throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR);
            }
            return searchResponse;
        } catch (Throwable t) {
            if (SdkSearchResponseUtils.isEmptySdkSearchParseFailure(t)) {
                return SdkSearchResponseUtils.emptySearchResponse();
            }
            throw new OpenSearchStatusException("Failed to parse search response", RestStatus.INTERNAL_SERVER_ERROR, t);
        }
    }

    private Map<String, Object> addTenantIdField(Map<String, Object> sourceAsMap, String tenantId) {
        if (tenantId == null) {
            return sourceAsMap;
        }
        Map<String, Object> updatedSource = new LinkedHashMap<>(sourceAsMap);
        updatedSource.put(CommonName.TENANT_ID_FIELD, tenantId);
        return updatedSource;
    }
}
