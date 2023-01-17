package org.opensearch.forecast.util;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_FIND_FORECASTER_MSG;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_GET_USER_INFO;
import static org.opensearch.forecast.constant.ForecastCommonMessages.NO_PERMISSION_TO_ACCESS_FORECASTER;

import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class ForecastParseUtils {
    private static final Logger logger = LogManager.getLogger(ForecastParseUtils.class);

    public static boolean checkFilterByBackendRoles(User requestedUser, ActionListener listener) {
        if (requestedUser == null) {
            return false;
        }
        if (requestedUser.getBackendRoles().isEmpty()) {
            listener
                .onFailure(
                    new TimeSeriesException(
                        "Filter by backend roles is enabled and User " + requestedUser.getName() + " does not have backend roles configured"
                    )
                );
            return false;
        }
        return true;
    }

    /**
     * If filterByBackendRole is true, get forecaster and check if the user has permissions to access the forecaster,
     * then execute function; otherwise, get forecaster and execute function
     * @param requestUser user from request
     * @param forecasterId forecaster id
     * @param listener action listener
     * @param function consumer function
     * @param client client
     * @param clusterService cluster service
     * @param xContentRegistry XContent registry
     * @param filterByBackendRole filter by backend role or not
     */
    public static void getForecaster(
        User requestUser,
        String forecasterId,
        ActionListener<? extends ActionResponse> listener,
        Consumer<Forecaster> function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole
    ) {
        if (clusterService.state().metadata().indices().containsKey(ForecastIndex.CONFIG.getIndexName())) {
            GetRequest request = new GetRequest(ForecastIndex.CONFIG.getIndexName()).id(forecasterId);
            client
                .get(
                    request,
                    ActionListener
                        .wrap(
                            response -> onGetAdResponse(
                                response,
                                requestUser,
                                forecasterId,
                                listener,
                                function,
                                xContentRegistry,
                                filterByBackendRole
                            ),
                            exception -> {
                                logger.error("Failed to get forecaster: " + forecasterId, exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } else {
            listener.onFailure(new IndexNotFoundException(ForecastIndex.CONFIG.getIndexName()));
        }
    }

    public static void onGetAdResponse(
        GetResponse response,
        User requestUser,
        String forecastId,
        ActionListener<? extends ActionResponse> listener,
        Consumer<Forecaster> function,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole
    ) {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Forecaster forecaster = Forecaster.parse(parser);
                User resourceUser = forecaster.getUser();

                if (!filterByBackendRole || checkUserPermissions(requestUser, resourceUser, forecastId)) {
                    function.accept(forecaster);
                } else {
                    logger.debug("User: " + requestUser.getName() + " does not have permissions to access forecaster: " + forecastId);
                    listener.onFailure(new TimeSeriesException(NO_PERMISSION_TO_ACCESS_FORECASTER + forecastId));
                }
            } catch (Exception e) {
                listener.onFailure(new TimeSeriesException(FAIL_TO_GET_USER_INFO + forecastId));
            }
        } else {
            listener.onFailure(new ResourceNotFoundException(forecastId, FAIL_TO_FIND_FORECASTER_MSG + forecastId));
        }
    }

    private static boolean checkUserPermissions(User requestedUser, User resourceUser, String forecasterId) throws Exception {
        if (resourceUser.getBackendRoles() == null || requestedUser.getBackendRoles() == null) {
            return false;
        }
        // Check if requested user has backend role required to access the resource
        for (String backendRole : requestedUser.getBackendRoles()) {
            if (resourceUser.getBackendRoles().contains(backendRole)) {
                logger
                    .debug(
                        "User: "
                            + requestedUser.getName()
                            + " has backend role: "
                            + backendRole
                            + " permissions to access forecaster: "
                            + forecasterId
                    );
                return true;
            }
        }
        return false;
    }

    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        logger.debug("Filtering result by " + userStr);
        return User.parse(userStr);
    }
}
