/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler.store;

import java.io.IOException;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.rest.handler.store.SDKDataManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

/**
 * AD-specific SDK data management that wires AD index names and dummy result content.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Client required by remote SDK library.")
public class ADSdkDataManagement extends SDKDataManagement<ADIndex, AnomalyResult> {
    private static final String DUMMY_RESULT_BODY = buildDummyResultBody();

    public ADSdkDataManagement(Client client, NamedXContentRegistry xContentRegistry, Settings settings, ClusterService clusterService) {
        super(
            ADIndex.CONFIG.getIndexName(),
            ADIndex.STATE.getIndexName(),
            ADIndex.CHECKPOINT.getIndexName(),
            client,
            xContentRegistry,
            ADCommonName.AD_THREAD_POOL_NAME,
            AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED,
            settings,
            AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
            AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
            ADIndex.RESULT.getMapping(),
            ADCommonName.DUMMY_AD_RESULT_ID,
            DUMMY_RESULT_BODY,
            ADIndex.class,
            ADCommonName.CUSTOM_RESULT_INDEX_PREFIX,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            clusterService,
            TimeSeriesSettings.MAX_CONCURRENT_SDK_INDEX_MAPPING_UPDATES
        );
    }

    private static String buildDummyResultBody() {
        try {
            AnomalyResult dummyResult = AnomalyResult.getDummyResult();
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            dummyResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to build AD dummy result body", e);
        }
    }
}
