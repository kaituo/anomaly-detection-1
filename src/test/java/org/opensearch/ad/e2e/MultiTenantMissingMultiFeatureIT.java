/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.client.RestClient;

public class MultiTenantMissingMultiFeatureIT extends MissingMultiFeatureIT {

    private final String multiTenantId = "tenant-missing-multi-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
    private final String datasetName = "missing-multi-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);

    @Override
    protected String tenantId() {
        return multiTenantId;
    }

    @Override
    protected RestClient ingestClient() throws IOException {
        return modelDataClient();
    }

    @Override
    protected RestClient anomalyResultClient(String detectorId, RestClient client) {
        try {
            return modelDataClient();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create model result client", e);
        }
    }

    @Override
    protected String datasetName() {
        return datasetName;
    }
}
