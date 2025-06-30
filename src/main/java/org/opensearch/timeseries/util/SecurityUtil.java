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

package org.opensearch.timeseries.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.profiles.ProfileFileSupplier;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class SecurityUtil {
    private static final String PROFILE_REFRESH_INTERVAL_SECONDS_PROPERTY = "aws.profile.refresh_interval_seconds";
    private static final String CREDENTIAL_PROCESS_SHELL_PROPERTY = "aws.profile.credential_process_shell";
    private static final long DEFAULT_PROFILE_REFRESH_INTERVAL_SECONDS = TimeUnit.MINUTES.toSeconds(20);
    private static final long CREDENTIAL_PROCESS_TIMEOUT_SECONDS = 60;
    private static final long EXPIRATION_REFRESH_BUFFER_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /**
     * @param userObj the last user who edited the detector config
     * @param settings Node settings
     * @return converted user for bwc if necessary
     */
    private static User getAdjustedUserBWC(User userObj, Settings settings) {
        /*
         * We need to handle 3 cases:
         * 1. Detectors created by older versions and never updated. These detectors wont have User details in the
         * detector object. `detector.user` will be null. Insert `all_access, AmazonES_all_access` role.
         * 2. Detectors are created when security plugin is disabled, these will have empty User object.
         * (`detector.user.name`, `detector.user.roles` are empty )
         * 3. Detectors are created when security plugin is enabled, these will have an User object.
         * This will inject user role and check if the user role has permissions to call the execute
         * Anomaly Result API.
         */
        String user;
        List<String> roles;
        if (userObj == null) {
            // It's possible that user create domain with security disabled, then enable security
            // after upgrading. This is for BWC, for old detectors which created when security
            // disabled, the user will be null.
            // This is a huge promotion in privileges. To prevent a caller code from making a mistake and pass a null object,
            // we make the method private and only allow fetching user object from detector or job configuration (see the public
            // access methods with the same name).
            user = "";
            roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
            return new User(user, Collections.emptyList(), roles, Collections.emptyList());
        } else {
            return userObj;
        }
    }

    /**
     * *
     * @param config analysis config
     * @param settings Node settings
     * @return user recorded by a detector. Made adjstument for BWC (backward-compatibility) if necessary.
     */
    public static User getUserFromConfig(Config config, Settings settings) {
        return getAdjustedUserBWC(config.getUser(), settings);
    }

    /**
     * *
     * @param detectorJob Detector Job
     * @param settings Node settings
     * @return user recorded by a detector job
     */
    public static User getUserFromJob(Job detectorJob, Settings settings) {
        return getAdjustedUserBWC(detectorJob.getUser(), settings);
    }

    public static AwsCredentialsProvider createCredentialsProvider() {
        String profileName = System.getProperty("aws.profile");
        if (profileName == null || profileName.isBlank()) {
            profileName = System.getenv("AWS_PROFILE");
        }
        if (profileName == null || profileName.isBlank()) {
            profileName = System.getenv("AWS_DEFAULT_PROFILE");
        }

        AwsCredentialsProvider profileProvider = new RefreshingProfileCredentialsProvider(profileName);

        AwsCredentialsProviderChain.Builder credentialsProviderChain = AwsCredentialsProviderChain.builder();
        if (profileName != null && profileName.isBlank() == false) {
            credentialsProviderChain.addCredentialsProvider(profileProvider);
        }
        credentialsProviderChain
            .addCredentialsProvider(SystemPropertyCredentialsProvider.create())
            .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create());
        if (profileName == null || profileName.isBlank()) {
            credentialsProviderChain.addCredentialsProvider(profileProvider);
        }
        return credentialsProviderChain
            .addCredentialsProvider(ContainerCredentialsProvider.builder().build())
            .addCredentialsProvider(InstanceProfileCredentialsProvider.create())
            .build();
    }

    /**
     * Assume {@code roleArn} <b>without</b> an {@code sts:ExternalId} — a thin overload that delegates
     * to {@link #createAssumeRoleCredentialsProvider(String, String, String, String)} with a
     * {@code null} external ID.
     *
     * <p>This overload is intentionally kept (not folded into the 4-arg form) because the external-ID
     * requirement is scoped to a single role, not to all cross-account assumes. Only
     * {@code ScheduleManagerRole} carries a {@code StringEquals: { sts:ExternalId }} condition in its
     * trust policy (added by the CDK EventBridgeCell stack); the shipped alerting fix that introduced
     * this — <a href="https://code.amazon.com/reviews/CR-284480892">CR-284480892</a> — likewise added
     * the external ID to only the scheduler-routing assume path and nothing else.
     *
     * <p>The other callers of this method assume roles that have <em>no</em> external-ID condition, so
     * they must NOT pass the scheduler's external ID:
     * <ul>
     *   <li>{@code AossDirectSigningClientFactory} — assumes the customer-collection signing role
     *       ({@code plugins.timeseries.background_job.assume_role_arn}); defaults to empty/unset, so no
     *       assume even occurs in current cells.</li>
     *   <li>integration-test setup assumes.</li>
     * </ul>
     * Removing this overload would force those unrelated paths to adopt the scheduler's external ID,
     * which would be semantically wrong and could break their assumes. Use the 4-arg overload only for
     * the {@code ScheduleManagerRole} scheduler-routing path.
     */
    public static AwsCredentialsProvider createAssumeRoleCredentialsProvider(String region, String roleArn, String roleSessionName) {
        return createAssumeRoleCredentialsProvider(region, roleArn, roleSessionName, null);
    }

    /**
     * Build an {@link AwsCredentialsProvider} that assumes {@code roleArn}, optionally passing an
     * {@code externalId} on the {@code sts:AssumeRole} request.
     *
     * <p>When {@code externalId} is non-blank it is included in the {@link AssumeRoleRequest}; the
     * target role's trust policy can then require {@code sts:ExternalId} to match. This guards
     * against the confused-deputy vector where the target account is request-influenced (e.g. via
     * the {@code x-scheduler-account-id} header): without a matching external ID, STS rejects the
     * assume even if the principal is trusted. See CR-284480892.
     *
     * <p>Callers that need to enforce the presence of an external ID (cross-account scheduler
     * routing) must validate non-blankness before calling and fail closed; callers assuming roles
     * that do not require an external ID (e.g. same-trust background jobs) pass {@code null}.
     */
    public static AwsCredentialsProvider createAssumeRoleCredentialsProvider(
        String region,
        String roleArn,
        String roleSessionName,
        String externalId
    ) {
        if (region == null || region.isBlank()) {
            throw SdkClientException.create("AWS region must be configured to assume role [" + roleArn + "].");
        }
        if (roleArn == null || roleArn.isBlank()) {
            throw SdkClientException.create("Role ARN must be configured for assume role credentials.");
        }
        if (roleSessionName == null || roleSessionName.isBlank()) {
            throw SdkClientException.create("Role session name must be configured for assume role credentials.");
        }

        StsClient stsClient = StsClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(Region.of(region))
            .credentialsProvider(createCredentialsProvider())
            .build();

        AssumeRoleRequest.Builder assumeRoleRequest = AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(roleSessionName);
        if (externalId != null && externalId.isBlank() == false) {
            assumeRoleRequest.externalId(externalId);
        }

        return StsAssumeRoleCredentialsProvider.builder().stsClient(stsClient).refreshRequest(assumeRoleRequest.build()).build();
    }

    private static class RefreshingProfileCredentialsProvider implements AwsCredentialsProvider, AutoCloseable {
        private final String profileName;
        private final long refreshIntervalMillis;
        private volatile ProfileCredentialsProvider delegate;
        private volatile AwsCredentials cachedProcessCredentials;
        private volatile boolean usingCredentialProcess;
        private volatile long refreshAfterMillis;

        private RefreshingProfileCredentialsProvider(String profileName) {
            this.profileName = profileName;
            long refreshIntervalSeconds = Long.getLong(PROFILE_REFRESH_INTERVAL_SECONDS_PROPERTY, DEFAULT_PROFILE_REFRESH_INTERVAL_SECONDS);
            this.refreshIntervalMillis = TimeUnit.SECONDS.toMillis(Math.max(0L, refreshIntervalSeconds));
        }

        @Override
        public AwsCredentials resolveCredentials() {
            String credentialProcess = readAwsProfileSettings().get("credential_process");
            if (credentialProcess != null && credentialProcess.isBlank() == false) {
                return currentProcessCredentials(credentialProcess);
            }
            return currentDelegate().resolveCredentials();
        }

        private AwsCredentials currentProcessCredentials(String credentialProcess) {
            AwsCredentials current = cachedProcessCredentials;
            long now = System.currentTimeMillis();
            if (usingCredentialProcess && current != null && now < refreshAfterMillis) {
                return current;
            }
            return refreshProcessCredentials(credentialProcess, now);
        }

        private synchronized AwsCredentials refreshProcessCredentials(String credentialProcess, long now) {
            AwsCredentials current = cachedProcessCredentials;
            if (usingCredentialProcess && current != null && now < refreshAfterMillis) {
                return current;
            }

            closeDelegate();
            ProcessCredentials refreshed = resolveCredentialProcess(credentialProcess);
            cachedProcessCredentials = refreshed.credentials();
            usingCredentialProcess = true;
            refreshAfterMillis = nextRefreshMillis(now, refreshed.expiration());
            return cachedProcessCredentials;
        }

        private ProfileCredentialsProvider currentDelegate() {
            ProfileCredentialsProvider current = delegate;
            long now = System.currentTimeMillis();
            if (usingCredentialProcess || current == null || now >= refreshAfterMillis) {
                return refreshDelegate(now);
            }
            return current;
        }

        private synchronized ProfileCredentialsProvider refreshDelegate(long now) {
            ProfileCredentialsProvider current = delegate;
            if (usingCredentialProcess == false && current != null && now < refreshAfterMillis) {
                return current;
            }

            ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider
                .builder()
                .profileFile(ProfileFileSupplier.defaultSupplier());
            if (profileName != null && profileName.isBlank() == false) {
                builder.profileName(profileName);
            }

            ProfileCredentialsProvider refreshed = builder.build();
            cachedProcessCredentials = null;
            usingCredentialProcess = false;
            delegate = refreshed;
            refreshAfterMillis = now + refreshIntervalMillis;
            closeDelegate(current);
            return refreshed;
        }

        private Map<String, String> readAwsProfileSettings() {
            String requestedProfileName = configuredProfileName();
            Map<String, String> profileSettings = new HashMap<>();
            profileSettings.putAll(readAwsProfileFile(awsConfigFile(), requestedProfileName, true));
            profileSettings.putAll(readAwsProfileFile(awsCredentialsFile(), requestedProfileName, false));
            return profileSettings;
        }

        private String configuredProfileName() {
            return profileName == null || profileName.isBlank() ? "default" : profileName;
        }

        private Path awsConfigFile() {
            String configuredPath = firstNonBlank(System.getProperty("aws.configFile"), System.getenv("AWS_CONFIG_FILE"));
            return configuredPath == null ? Paths.get(System.getProperty("user.home"), ".aws", "config") : Paths.get(configuredPath);
        }

        private Path awsCredentialsFile() {
            String configuredPath = firstNonBlank(
                System.getProperty("aws.sharedCredentialsFile"),
                System.getenv("AWS_SHARED_CREDENTIALS_FILE")
            );
            return configuredPath == null ? Paths.get(System.getProperty("user.home"), ".aws", "credentials") : Paths.get(configuredPath);
        }

        private Map<String, String> readAwsProfileFile(Path profileFile, String requestedProfileName, boolean configFile) {
            if (Files.exists(profileFile) == false) {
                return Collections.emptyMap();
            }

            Map<String, String> profileSettings = new HashMap<>();
            boolean inRequestedProfile = false;
            try {
                for (String rawLine : Files.readAllLines(profileFile, StandardCharsets.UTF_8)) {
                    String line = rawLine.trim();
                    if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                        continue;
                    }

                    if (line.startsWith("[") && line.endsWith("]")) {
                        String section = line.substring(1, line.length() - 1).trim();
                        inRequestedProfile = awsProfileSectionMatches(section, requestedProfileName, configFile);
                        continue;
                    }

                    if (inRequestedProfile == false) {
                        continue;
                    }

                    int separatorIndex = line.indexOf('=');
                    if (separatorIndex <= 0) {
                        continue;
                    }

                    profileSettings.put(line.substring(0, separatorIndex).trim(), line.substring(separatorIndex + 1).trim());
                }
            } catch (IOException e) {
                throw SdkClientException.create("Failed to read AWS profile file [" + profileFile + "].", e);
            }
            return profileSettings;
        }

        private boolean awsProfileSectionMatches(String section, String requestedProfileName, boolean configFile) {
            if ("default".equals(requestedProfileName)) {
                return "default".equals(section) || "profile default".equals(section);
            }
            return requestedProfileName.equals(section) || (configFile && ("profile " + requestedProfileName).equals(section));
        }

        private ProcessCredentials resolveCredentialProcess(String credentialProcess) {
            Process process = null;
            try {
                process = new ProcessBuilder(processShell(), "-lc", credentialProcess).redirectErrorStream(true).start();
                boolean finished = process.waitFor(CREDENTIAL_PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (finished == false) {
                    process.destroyForcibly();
                    throw SdkClientException
                        .create(
                            "Timed out while resolving AWS credentials from profile [" + configuredProfileName() + "] credential_process."
                        );
                }

                String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (process.exitValue() != 0) {
                    throw SdkClientException
                        .create(
                            "Failed to resolve AWS credentials from profile ["
                                + configuredProfileName()
                                + "] credential_process. Output: "
                                + abbreviate(output.trim(), 2_000)
                        );
                }
                return parseCredentialProcessOutput(output);
            } catch (IOException e) {
                throw SdkClientException
                    .create(
                        "Failed to execute AWS profile ["
                            + configuredProfileName()
                            + "] credential_process with shell ["
                            + processShell()
                            + "]: "
                            + e.getMessage(),
                        e
                    );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw SdkClientException
                    .create(
                        "Interrupted while resolving AWS credentials from profile [" + configuredProfileName() + "] credential_process.",
                        e
                    );
            } finally {
                if (process != null) {
                    process.destroy();
                }
            }
        }

        private ProcessCredentials parseCredentialProcessOutput(String output) {
            try {
                JsonObject parsed = JsonParser.parseString(output).getAsJsonObject();
                String accessKeyId = stringField(parsed, "AccessKeyId", "accessKeyId");
                String secretAccessKey = stringField(parsed, "SecretAccessKey", "secretAccessKey");
                String sessionToken = stringField(parsed, "SessionToken", "sessionToken");
                if (accessKeyId == null || accessKeyId.isBlank() || secretAccessKey == null || secretAccessKey.isBlank()) {
                    throw SdkClientException
                        .create(
                            "AWS profile [" + configuredProfileName() + "] credential_process did not return AccessKeyId/SecretAccessKey."
                        );
                }

                AwsCredentials credentials = sessionToken == null || sessionToken.isBlank()
                    ? AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                    : AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
                return new ProcessCredentials(credentials, parseExpiration(stringField(parsed, "Expiration", "expiration")));
            } catch (IllegalStateException | JsonSyntaxException e) {
                throw SdkClientException
                    .create("AWS profile [" + configuredProfileName() + "] credential_process returned invalid JSON.", e);
            }
        }

        private Instant parseExpiration(String expiration) {
            if (expiration == null || expiration.isBlank()) {
                return null;
            }
            try {
                return Instant.parse(expiration);
            } catch (DateTimeParseException e) {
                return null;
            }
        }

        private String stringField(JsonObject parsed, String firstFieldName, String secondFieldName) {
            String firstValue = stringField(parsed, firstFieldName);
            if (firstValue != null) {
                return firstValue;
            }
            return stringField(parsed, secondFieldName);
        }

        private String stringField(JsonObject parsed, String fieldName) {
            if (parsed.has(fieldName) == false || parsed.get(fieldName).isJsonNull()) {
                return null;
            }
            return parsed.get(fieldName).getAsString().trim();
        }

        private long nextRefreshMillis(long now, Instant expiration) {
            long refreshAt = now + refreshIntervalMillis;
            if (expiration != null) {
                refreshAt = Math.min(refreshAt, expiration.toEpochMilli() - EXPIRATION_REFRESH_BUFFER_MILLIS);
            }
            return Math.max(now, refreshAt);
        }

        private String processShell() {
            String configuredShell = System.getProperty(CREDENTIAL_PROCESS_SHELL_PROPERTY);
            if (configuredShell != null && configuredShell.isBlank() == false) {
                return configuredShell;
            }
            Path zsh = Paths.get("/bin/zsh");
            return Files.isExecutable(zsh) ? zsh.toString() : "/bin/sh";
        }

        private String firstNonBlank(String first, String second) {
            if (first != null && first.isBlank() == false) {
                return first;
            }
            if (second != null && second.isBlank() == false) {
                return second;
            }
            return null;
        }

        private String abbreviate(String value, int maxLength) {
            if (value.length() <= maxLength) {
                return value;
            }
            return value.substring(0, maxLength) + "...";
        }

        private void closeDelegate() {
            ProfileCredentialsProvider current = delegate;
            delegate = null;
            closeDelegate(current);
        }

        private void closeDelegate(ProfileCredentialsProvider current) {
            if (current != null) {
                current.close();
            }
        }

        @Override
        public synchronized void close() {
            closeDelegate();
            cachedProcessCredentials = null;
            usingCredentialProcess = false;
            refreshAfterMillis = 0L;
        }

        private record ProcessCredentials(AwsCredentials credentials, Instant expiration) {
        }
    }
}
