/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.client.RestClient;
import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class SigningRestClientProviderTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        SigningRestClientProvider.closeAll();
        super.tearDown();
    }

    public void testCacheSeparatesCredentialsProvidersForSameEndpointAndRegion() {
        String endpoint = "http://127.0.0.1:19291";
        AwsCredentialsProvider firstProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("first", "secret"));
        AwsCredentialsProvider secondProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("second", "secret"));

        RestClient firstClient = SigningRestClientProvider.getRestClient(endpoint, "us-west-2", firstProvider, "aoss");
        RestClient firstClientAgain = SigningRestClientProvider.getRestClient(endpoint, "us-west-2", firstProvider, "aoss");
        RestClient secondClient = SigningRestClientProvider.getRestClient(endpoint, "us-west-2", secondProvider, "aoss");

        assertSame(firstClient, firstClientAgain);
        assertNotSame(firstClient, secondClient);
    }

    public void testCacheSeparatesServiceNamesForSameEndpointRegionAndCredentialsProvider() {
        String endpoint = "http://127.0.0.1:19292";
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));

        RestClient aossClient = SigningRestClientProvider.getRestClient(endpoint, "us-west-2", credentialsProvider, "aoss");
        RestClient esClient = SigningRestClientProvider.getRestClient(endpoint, "us-west-2", credentialsProvider, "es");

        assertNotSame(aossClient, esClient);
    }
}
