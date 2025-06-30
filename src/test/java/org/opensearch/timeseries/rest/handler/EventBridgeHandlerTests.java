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

package org.opensearch.timeseries.rest.handler;

import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;

public class EventBridgeHandlerTests extends AbstractTimeSeriesTest {

    public void testResolveConfigScheduleGroupFallsBackToAnalysisTypeName() {
        assertEquals("ad", EventBridgeHandler.resolveConfigScheduleGroup(null, AnalysisType.AD));
        assertEquals("forecast", EventBridgeHandler.resolveConfigScheduleGroup("   ", AnalysisType.FORECAST));
    }

    public void testResolveConfigScheduleGroupUsesConfiguredValue() {
        assertEquals("existing-group", EventBridgeHandler.resolveConfigScheduleGroup(" existing-group ", AnalysisType.AD));
    }

    public void testResolveMaintenanceScheduleGroupFallsBackToTimeseries() {
        assertEquals("timeseries", EventBridgeHandler.resolveMaintenanceScheduleGroup(null));
        assertEquals("timeseries", EventBridgeHandler.resolveMaintenanceScheduleGroup("   "));
    }

    public void testResolveMaintenanceScheduleGroupUsesConfiguredValue() {
        assertEquals("existing-group", EventBridgeHandler.resolveMaintenanceScheduleGroup(" existing-group "));
    }
}
