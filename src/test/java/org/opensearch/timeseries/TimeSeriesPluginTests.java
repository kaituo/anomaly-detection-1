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

package org.opensearch.timeseries;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.opensearch.ad.ADUnitTestCase;

import io.protostuff.LinkedBuffer;

public class TimeSeriesPluginTests extends ADUnitTestCase {
    TimeSeriesAnalyticsPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new TimeSeriesAnalyticsPlugin();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        plugin.close();
    }

    /**
     * We have legacy setting. TimeSeriesAnalyticsPlugin's createComponents can trigger
     * warning when using these legacy settings.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testDeserializeRCFBufferPool() throws Exception {
        plugin.serializeRCFBufferPool = plugin.createSerializeRCFBufferPool();
        GenericObjectPool<LinkedBuffer> deserializeRCFBufferPool = plugin.serializeRCFBufferPool;
        deserializeRCFBufferPool.addObject();
        LinkedBuffer buffer = deserializeRCFBufferPool.borrowObject();
        assertTrue(null != buffer);
    }

    public void testOverriddenJobTypeAndIndex() {
        assertEquals("opensearch_time_series_analytics", plugin.getJobType());
        assertEquals(".opendistro-anomaly-detector-jobs", plugin.getJobIndex());
    }

}
