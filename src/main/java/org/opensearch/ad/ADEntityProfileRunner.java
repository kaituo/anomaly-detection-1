/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;

public class ADEntityProfileRunner extends EntityProfileRunner {

    public ADEntityProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager stateManager,
        long requiredSamples
    ) {
        super(
            nodeCommunicator,
            dataAccess,
            stateManager,
            requiredSamples,
            ADNumericSetting.maxCategoricalFields(),
            AnalysisType.AD,
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            AnomalyResult.DETECTOR_ID_FIELD
        );
    }
}
