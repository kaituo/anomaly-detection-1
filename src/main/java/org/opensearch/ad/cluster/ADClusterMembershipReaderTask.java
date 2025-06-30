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

package org.opensearch.ad.cluster;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.timeseries.cluster.ClusterMembershipReaderTask;

public class ADClusterMembershipReaderTask extends ClusterMembershipReaderTask {

    public ADClusterMembershipReaderTask() {
        super(ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME);
    }
}
