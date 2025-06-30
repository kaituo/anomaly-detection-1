/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.timeseries.util.TenantAwareHelper;

/**
 * Resolves the endpoint to contact for a given data source when issuing HTTP requests.
 * <p>
 * Implementations may throw {@link org.opensearch.timeseries.common.exception.EndRunException}
 * with {@code endNow=false} when the data source cannot be found (e.g., 404 from a remote lookup).
 * Using {@code endNow=false} allows the system to retry rather than permanently halting the
 * detector/forecaster.
 */
public interface DataSourceEndpointResolver {
    /**
     * Resolve endpoint from tenant id.
     *
     * @param tenantId tenant id; must not be {@code null} when the call targets a user
     *                 index (result index, source index, etc.)
     * @return endpoint string
     */
    default String resolve(String tenantId) {
        Pair<String, String> tenantComponents = TenantAwareHelper.parseTenantId(tenantId);
        return resolve(tenantComponents.getKey(), tenantComponents.getValue());
    }

    /**
     * Resolve endpoint from application id and data source id.
     *
     * @param applicationId application id
     * @param dataSourceId  data source id
     * @return endpoint string
     */
    String resolve(String applicationId, String dataSourceId);
}
