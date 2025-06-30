/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public abstract class BaseSearchConfigInfoTransportAction extends
    HandledTransportAction<SearchConfigInfoRequest, SearchConfigInfoResponse> {
    private static final Logger LOG = LogManager.getLogger(BaseSearchConfigInfoTransportAction.class);
    private final DataAccess dataAccess;
    protected String configIndexName;
    protected final Settings settings;
    private final RunContext runContext;

    public BaseSearchConfigInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        DataAccess dataAccess,
        String searchConfigActionName,
        String configIndexName,
        Settings settings,
        RunContext runContext
    ) {
        super(searchConfigActionName, transportService, actionFilters, SearchConfigInfoRequest::new);
        this.dataAccess = dataAccess;
        this.configIndexName = configIndexName;
        this.settings = settings;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, SearchConfigInfoRequest request, ActionListener<SearchConfigInfoResponse> actionListener) {
        String name = request.getName();
        String rawPath = request.getRawPath();
        String tenantId = request.getTenantId();
        ActionListener<SearchConfigInfoResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_GET_CONFIG_INFO);

        try {
            TenantAwareHelper.validateTenantId(tenantId, settings, getMultiTenancyEnabledSetting());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext.runWithSystemAuth(() -> {
            SearchRequest searchRequest = new SearchRequest().indices(configIndexName);
            if (rawPath.endsWith(RestHandlerUtils.COUNT)) {
                // Count detectors
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchRequest.source(searchSourceBuilder);
                dataAccess.search(searchRequest, TenantContext.user(tenantId), new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        SearchConfigInfoResponse response = new SearchConfigInfoResponse(
                            searchResponse.getHits().getTotalHits().value(),
                            false
                        );
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, false);
                            listener.onResponse(response);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                // Match name with existing detectors
                TermsQueryBuilder query = QueryBuilders.termsQuery("name.keyword", name);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
                searchRequest.source(searchSourceBuilder);
                dataAccess.search(searchRequest, TenantContext.user(tenantId), new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        boolean nameExists = false;
                        nameExists = searchResponse.getHits().getTotalHits().value() > 0;
                        SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, nameExists);
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, false);
                            listener.onResponse(response);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            }
        }, exception -> {
            LOG.error(exception);
            listener.onFailure(exception);
        });
    }

    /**
     * Returns the setting that indicates if multi-tenancy is enabled.
     * Subclasses must implement this to provide the appropriate setting.
     */
    protected abstract Setting<Boolean> getMultiTenancyEnabledSetting();
}
