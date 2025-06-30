/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.sqs;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.sqs.SQSService;

public class ADSQSService extends SQSService {
    public ADSQSService(Settings settings) {
        super(settings, AnomalyDetectorSettings.SQS_QUEUE_URL.get(settings));
    }
}
