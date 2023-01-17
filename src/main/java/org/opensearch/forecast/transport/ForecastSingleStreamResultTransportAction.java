package org.opensearch.forecast.transport;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.transport.TimeSeriesEntityResultProcessor;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamResultTransportAction extends HandledTransportAction<ForecastSingleStreamResultRequest, AcknowledgedResponse> {
    private static final Logger LOG = LogManager.getLogger(EntityForecastResultTransportAction.class);
    private TimeSeriesCircuitBreakerService circuitBreakerService;
    private EntityCacheProvider<RCFCaster, ForecastModelState<RCFCaster>> cache;
    private final ForecastNodeStateManager stateManager;
    private ThreadPool threadPool;

    @Override
    protected void doExecute(Task task, ForecastSingleStreamResultRequest request,
            ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            threadPool.executor(TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME).execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener
                .onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String forecasterId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(forecasterId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", forecasterId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, forecasterId);
            }

            stateManager.getConfig(forecasterId, intervalDataProcessor.onGetConfig(listener, forecasterId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }

}
