/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.AbstractSearchAction;

public abstract class AbstractForecastSearchAction<T extends ToXContentObject> extends AbstractSearchAction<T> {

    public AbstractForecastSearchAction(
        List<String> urlPaths,
        List<Pair<String, String>> deprecatedPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType
    ) {
        super(
            urlPaths,
            deprecatedPaths,
            index,
            clazz,
            actionType,
            ForecastEnabledSetting::isForecastEnabled,
            ForecastCommonMessages.DISABLED_ERR_MSG
        );
    }
}
