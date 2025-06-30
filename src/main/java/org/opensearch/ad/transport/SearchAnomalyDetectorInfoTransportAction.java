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

package org.opensearch.ad.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.transport.BaseSearchConfigInfoTransportAction;
import org.opensearch.transport.TransportService;

public class SearchAnomalyDetectorInfoTransportAction extends BaseSearchConfigInfoTransportAction {

    @Inject
    public SearchAnomalyDetectorInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        DataAccess dataAccess,
        Settings settings,
        RunContext runContext
    ) {
        super(
            transportService,
            actionFilters,
            dataAccess,
            SearchAnomalyDetectorInfoAction.NAME,
            ADCommonName.CONFIG_INDEX,
            settings,
            runContext
        );
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
    }
}
