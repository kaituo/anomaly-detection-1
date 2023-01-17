package org.opensearch.timeseries.caching;

import org.opensearch.timeseries.AnalysisModelSize;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;

public interface Cache <ModelStateType, ModelState extends TimeSeriesModelState<ModelStateType>> extends MaintenanceState, CleanState, AnalysisModelSize {
    /**
    *
    * @param config Analysis config
    * @param toUpdate Model state candidate
    * @return if we can host the given model state
    */
   boolean hostIfPossible(Config config, ModelState toUpdate);
}
