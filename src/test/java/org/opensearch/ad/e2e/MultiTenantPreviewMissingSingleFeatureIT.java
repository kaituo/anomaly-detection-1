/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.client.RestClient;

public class MultiTenantPreviewMissingSingleFeatureIT extends PreviewMissingSingleFeatureIT {

    private final String multiTenantId = "tenant-preview-missing-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
    private final String datasetName = "missing-preview-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);

    @Override
    protected String tenantId() {
        return multiTenantId;
    }

    @Override
    protected RestClient ingestClient() throws IOException {
        return modelDataClient();
    }

    @Override
    protected String datasetName() {
        return datasetName;
    }
}
