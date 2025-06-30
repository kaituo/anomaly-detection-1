/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.timeseries.util.ParseUtils.getResourceTypeFromClassName;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;

import java.time.Clock;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.Processor;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public abstract class BaseValidateConfigTransportAction<IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, ConfigStoreType extends DelegatingDataManagement<IndexType>>
    extends HandledTransportAction<ActionRequest, ValidateConfigResponse> {
    public static final Logger logger = LogManager.getLogger(BaseValidateConfigTransportAction.class);

    protected final ClusterService clusterService;
    protected final NamedXContentRegistry xContentRegistry;
    protected final DataManagementType indexManagement;
    protected final SearchFeatureDao searchFeatureDao;
    protected final NamedWriteableRegistry namedWriteableRegistry;
    protected volatile Boolean filterByEnabled;
    protected Clock clock;
    protected Settings settings;
    protected ValidationAspect validationAspect;
    protected ConfigStoreType configStore;
    protected final DataAccess dataAccess;
    protected final RunContext runContext;

    public BaseValidateConfigTransportAction(
        String actionName,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        DataManagementType indexManagement,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao,
        Setting<Boolean> filterByBackendRoleSetting,
        ValidationAspect validationAspect,
        ConfigStoreType configStore,
        NamedWriteableRegistry namedWriteableRegistry,
        DataAccess dataAccess,
        RunContext runContext
    ) {
        super(actionName, transportService, actionFilters, ValidateConfigRequest::new);
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.indexManagement = indexManagement;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.filterByEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByEnabled = it);
        this.searchFeatureDao = searchFeatureDao;
        this.clock = Clock.systemUTC();
        this.settings = settings;
        this.validationAspect = validationAspect;
        this.configStore = configStore;
        this.dataAccess = dataAccess;
        this.runContext = runContext;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<ValidateConfigResponse> listener) {
        ValidateConfigRequest request = ValidateConfigRequest.fromActionRequest(actionRequest, namedWriteableRegistry);
        User user = runContext.getUser();

        try {
            TenantAwareHelper.validateTenantId(request.getTenantId(), settings, getMultiTenancyEnabledSetting());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        stampRequestTenantId(request);

        runContext.runWithSystemAuth(context -> {
            String resourceType = getResourceTypeFromClassName(request.getConfig().getClass().getSimpleName());
            verifyResourceAccessAndProcessRequest(
                resourceType,
                () -> validateExecute(request, user, context, listener),
                () -> resolveUserAndExecute(user, listener, () -> validateExecute(request, user, context, listener))
            );
        }, exception -> {
            logger.error(exception);
            listener.onFailure(exception);
        });
    }

    private void stampRequestTenantId(ValidateConfigRequest request) {
        String tenantId = request.getTenantId();
        if (tenantId != null) {
            request.getConfig().setTenantId(tenantId);
        }
    }

    public void resolveUserAndExecute(User requestedUser, ActionListener<ValidateConfigResponse> listener, ExecutorFunction function) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users validating configs who do not have backend roles.
            if (filterByEnabled) {
                String error = checkFilterByBackendRoles(requestedUser);
                if (error != null) {
                    listener.onFailure(new TimeSeriesException(error));
                    return;
                }
            }
            // Validate analysis
            function.execute();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void checkIndicesAndExecute(
        List<String> indices,
        ExecutorFunction function,
        Config config,
        ActionListener<ValidateConfigResponse> listener
    ) {
        String tenantId = config.getTenantId();
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        dataAccess.search(searchRequest, TenantContext.user(config), ActionListener.wrap(r -> function.execute(), e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                // IndexNotFoundException is converted to a ADValidationException that gets
                // parsed to a ValidationIssue that is returned to
                // the user as a response indicating index doesn't exist
                ConfigValidationIssue issue = parseValidationException(
                    new ValidationException(CommonMessages.INDEX_NOT_FOUND, ValidationIssueType.INDICES, validationAspect)
                );
                listener.onResponse(new ValidateConfigResponse(issue));
                return;
            }
            logger.error(e);
            listener.onFailure(e);
        }));
    }

    protected Map<String, String> getFeatureSubIssuesFromErrorMessage(String errorMessage) {
        Map<String, String> result = new HashMap<>();
        String[] subIssueMessagesSuffix = errorMessage.split(", ");
        for (int i = 0; i < subIssueMessagesSuffix.length; i++) {
            String[] subIssueMsgs = subIssueMessagesSuffix[i].split(": ");
            // e.g., key: value: Feature max1 has issue: Data is most likely too sparse when given feature queries are applied. Consider
            // revising feature queries.
            result.put(subIssueMsgs[1], subIssueMsgs[0]);
        }
        return result;
    }

    public ConfigValidationIssue parseValidationException(ValidationException exception) {
        String originalErrorMessage = exception.getMessage();
        String errorMessage = "";
        Map<String, String> subIssues = null;
        IntervalTimeConfiguration intervalSuggestion = exception.getIntervalSuggestion();
        switch (exception.getType()) {
            case FEATURE_ATTRIBUTES:
                int firstLeftBracketIndex = originalErrorMessage.indexOf("[");
                int lastRightBracketIndex = originalErrorMessage.lastIndexOf("]");
                if (firstLeftBracketIndex != -1) {
                    // if feature issue messages are between square brackets like
                    // [Feature has issue: A, Feature has issue: B]
                    errorMessage = originalErrorMessage.substring(firstLeftBracketIndex + 1, lastRightBracketIndex);
                    subIssues = getFeatureSubIssuesFromErrorMessage(errorMessage);
                } else {
                    // features having issue like over max feature limit, duplicate feature name, etc.
                    errorMessage = originalErrorMessage;
                }
                break;
            case NAME:
            case CATEGORY:
            case DETECTION_INTERVAL:
            case FILTER_QUERY:
            case TIMEFIELD_FIELD:
            case SHINGLE_SIZE_FIELD:
            case WINDOW_DELAY:
            case RESULT_INDEX:
            case GENERAL_SETTINGS:
            case AGGREGATION:
            case TIMEOUT:
            case INDICES:
            case FORECAST_INTERVAL:
            case IMPUTATION:
            case HORIZON_SIZE:
            case RECENCY_EMPHASIS:
                errorMessage = originalErrorMessage;
                break;
        }
        return new ConfigValidationIssue(exception.getAspect(), exception.getType(), errorMessage, subIssues, intervalSuggestion);
    }

    public void validateExecute(
        ValidateConfigRequest request,
        User user,
        RunContext.RestorableContext storedContext,
        ActionListener<ValidateConfigResponse> listener
    ) {
        storedContext.restore();
        Config config = request.getConfig();
        ActionListener<ValidateConfigResponse> validateListener = ActionListener.wrap(response -> {
            // forcing response to be empty
            listener.onResponse(new ValidateConfigResponse((ConfigValidationIssue) null));
        }, exception -> {
            if (exception instanceof ValidationException) {
                // ADValidationException is converted as validation issues returned as response to user
                ConfigValidationIssue issue = parseValidationException((ValidationException) exception);
                listener.onResponse(new ValidateConfigResponse(issue));
                return;
            }
            logger.error(exception);
            listener.onFailure(exception);
        });
        checkIndicesAndExecute(config.getIndices(), () -> {
            try {
                createProcessor(config, request, user).start(validateListener);
            } catch (Exception exception) {
                String errorMessage = String
                    .format(Locale.ROOT, "Unknown exception caught while validating config %s", request.getConfig());
                logger.error(errorMessage, exception);
                listener.onFailure(exception);
            }
        }, config, listener);
    }

    protected abstract Processor<ValidateConfigResponse> createProcessor(Config config, ValidateConfigRequest request, User user);

    /**
     * Returns the setting that indicates if multi-tenancy is enabled.
     * Subclasses must implement this to provide the appropriate setting.
     */
    protected abstract Setting<Boolean> getMultiTenancyEnabledSetting();
}
