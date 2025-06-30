/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;

/**
 * Storage abstraction for AD/forecast config documents.
 */
public interface ConfigDocumentStore {
    void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener);

    void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener);

    void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener);

    void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener);
}
