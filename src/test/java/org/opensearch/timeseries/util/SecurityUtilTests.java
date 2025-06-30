/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class SecurityUtilTests extends OpenSearchTestCase {

    public void testCreateCredentialsProviderReloadsProfileFileChanges() throws Exception {
        Path credentialsFile = createTempDir().resolve("credentials");
        Path configFile = createTempDir().resolve("config");
        Files.writeString(configFile, "", StandardCharsets.UTF_8);
        writeCredentials(credentialsFile, "session-token-1");

        String originalSharedCredentialsFile = System.getProperty("aws.sharedCredentialsFile");
        String originalConfigFile = System.getProperty("aws.configFile");
        String originalProfile = System.getProperty("aws.profile");
        String originalRefreshInterval = System.getProperty("aws.profile.refresh_interval_seconds");

        try {
            System.setProperty("aws.sharedCredentialsFile", credentialsFile.toString());
            System.setProperty("aws.configFile", configFile.toString());
            System.setProperty("aws.profile", "default");
            System.setProperty("aws.profile.refresh_interval_seconds", "0");

            AwsCredentialsProvider provider = SecurityUtil.createCredentialsProvider();

            AwsSessionCredentials initialCredentials = asSessionCredentials(provider.resolveCredentials());
            assertEquals("session-token-1", initialCredentials.sessionToken());

            long currentLastModified = Files.getLastModifiedTime(credentialsFile).toMillis();
            writeCredentials(credentialsFile, "session-token-2");
            Files.setLastModifiedTime(credentialsFile, FileTime.fromMillis(currentLastModified + 2_000L));
            Thread.sleep(1_100L);

            AwsSessionCredentials refreshedCredentials = asSessionCredentials(provider.resolveCredentials());
            assertEquals("session-token-2", refreshedCredentials.sessionToken());
        } finally {
            restoreProperty("aws.sharedCredentialsFile", originalSharedCredentialsFile);
            restoreProperty("aws.configFile", originalConfigFile);
            restoreProperty("aws.profile", originalProfile);
            restoreProperty("aws.profile.refresh_interval_seconds", originalRefreshInterval);
        }
    }

    private AwsSessionCredentials asSessionCredentials(AwsCredentials credentials) {
        assertTrue(credentials instanceof AwsSessionCredentials);
        return (AwsSessionCredentials) credentials;
    }

    private void writeCredentials(Path credentialsFile, String sessionToken) throws Exception {
        Files
            .writeString(
                credentialsFile,
                "[default]\n"
                    + "aws_access_key_id = test-access-key\n"
                    + "aws_secret_access_key = test-secret-key\n"
                    + "aws_session_token = "
                    + sessionToken
                    + "\n",
                StandardCharsets.UTF_8
            );
    }

    private void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
