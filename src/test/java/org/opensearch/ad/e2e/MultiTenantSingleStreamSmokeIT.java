/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.IOException;
import java.util.Locale;

import org.apache.hc.core5.http.HttpHost;
import org.junit.After;
import org.opensearch.client.RestClient;

public class MultiTenantSingleStreamSmokeIT extends SingleStreamSmokeIT {
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";

    private final String multiTenantId = "tenant-single-stream-smoke-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
    private final String datasetName = "single-stream-smoke-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    private RestClient modelDataClient;

    @Override
    protected String tenantId() {
        return multiTenantId;
    }

    @Override
    protected RestClient ingestClient() throws IOException {
        if (modelDataClient == null) {
            modelDataClient = buildClient(restClientSettings(), modelHosts());
        }
        return modelDataClient;
    }

    @Override
    protected RestClient anomalyResultClient(String detectorId, RestClient client) {
        try {
            return ingestClient();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create model result client", e);
        }
    }

    @Override
    protected String datasetName() {
        return datasetName;
    }

    @After
    public void closeModelDataClient() throws IOException {
        if (modelDataClient != null) {
            modelDataClient.close();
            modelDataClient = null;
        }
    }

    private HttpHost[] modelHosts() {
        String cluster = System.getProperty(MODEL_CLUSTER_PROPERTY);
        if (cluster == null || cluster.isBlank()) {
            throw new IllegalStateException("Must specify [" + MODEL_CLUSTER_PROPERTY + "] to run " + getClass().getSimpleName());
        }

        String[] stringUrls = cluster.split(",");
        HttpHost[] hosts = new HttpHost[stringUrls.length];
        for (int i = 0; i < stringUrls.length; i++) {
            String stringUrl = stringUrls[i].trim();
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
            hosts[i] = buildHttpHost(host, port);
        }
        return hosts;
    }
}
