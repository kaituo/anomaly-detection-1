/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;

import java.util.Locale;

import org.opensearch.common.settings.Settings;

/**
 * Helpers for interpreting data-plane service settings.
 */
public final class DataPlaneServiceUtils {
    private static final String AOSS_SERVICE_NAME = "aoss";
    private static final String REMOTE_METADATA_SERVICE_NAME_SETTING = "plugins.anomaly_detection." + REMOTE_METADATA_SERVICE_NAME_KEY;

    private DataPlaneServiceUtils() {}

    /**
     * The current multi-tenant wiring uses the remote metadata service name setting as the
     * AWS signing service selector ({@code es} vs {@code aoss}). Until we have a dedicated
     * data-plane capability model, this is the least noisy signal for AOSS-specific REST behavior.
     */
    public static boolean isAossDataPlane(Settings settings) {
        if (settings == null) {
            return false;
        }
        String serviceName = settings.get(REMOTE_METADATA_SERVICE_NAME_SETTING);
        return serviceName != null && AOSS_SERVICE_NAME.equals(serviceName.toLowerCase(Locale.ROOT));
    }

    public static boolean isAossEndpoint(String endpoint) {
        return endpoint != null && endpoint.toLowerCase(Locale.ROOT).contains(".aoss.");
    }
}
