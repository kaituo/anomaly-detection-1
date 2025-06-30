/*
 * SPDX-License-Identifier: Apache-2.0
 * 
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * 
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 * 
 */

package org.opensearch.ad.rest.handler;

import java.time.Clock;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class ADEventBridgeHandler extends EventBridgeHandler {

    public ADEventBridgeHandler(Settings settings, Clock clock) {
        super(
            TimeSeriesSettings.REGION.get(settings),
            AnomalyDetectorSettings.AD_SQS_QUEUE_ARN.get(settings),
            AnomalyDetectorSettings.AD_SCHEDULER_ROLE_ARN.get(settings),
            AnalysisType.AD,
            clock
        );
    }
}
