/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.RestClient;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.TenantAwareHelper;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * SigV4-signed client factory for direct AOSS data-plane calls from background jobs.
 */
public class AossDirectSigningClientFactory implements DataPlaneClientFactory {
    private final String region;
    private final DataSourceEndpointResolver endpointResolver;
    /*
     * Workaround for AOSS data access policies: they do not accept a cross-account principal.
     * Use assumeRoleArn only when there is no alternative without assuming the customer collection
     * role in the service account. If such a workaround exists, do not use assumeRoleArn.
     */
    private final String assumeRoleArn;
    private final String signingServiceName;
    private final AwsCredentialsProvider credentialsProvider;
    private final boolean endpointRegionSigningEnabled;

    public AossDirectSigningClientFactory(
        String region,
        DataSourceEndpointResolver endpointResolver,
        String assumeRoleArn,
        String signingServiceName,
        boolean endpointRegionSigningEnabled
    ) {
        this.region = Objects.requireNonNull(region, "region must not be null");
        if (this.region.isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
        this.assumeRoleArn = normalize(assumeRoleArn);
        this.signingServiceName = normalize(signingServiceName);
        this.endpointRegionSigningEnabled = endpointRegionSigningEnabled;
        this.credentialsProvider = this.assumeRoleArn == null
            ? null
            : SecurityUtil.createAssumeRoleCredentialsProvider(this.region, this.assumeRoleArn, "timeseries-background-job");
    }

    @Override
    public RestClient getClient(String tenantId, String dataSourceId) {
        return getResolvedClient(tenantId, dataSourceId).restClient();
    }

    @Override
    public ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
        String endpoint = resolveEndpoint(tenantId, dataSourceId);
        String signingRegion = endpointRegionSigningEnabled
            ? EndpointRegionResolver.resolveRegionFromEndpointOrDefault(endpoint, region)
            : region;
        return new ResolvedClient(
            SigningRestClientProvider.getRestClient(endpoint, signingRegion, credentialsProvider, signingServiceName),
            endpoint
        );
    }

    private String resolveEndpoint(String tenantId, String dataSourceId) {
        String applicationId = tenantId == null ? null : TenantAwareHelper.parseTenantId(tenantId).applicationId();
        return endpointResolver.resolve(applicationId, dataSourceId);
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
