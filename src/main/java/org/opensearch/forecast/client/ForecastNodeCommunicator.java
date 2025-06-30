/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.forecast.client;

import org.opensearch.timeseries.client.NodeCommunicator;

/**
 * DI (dependency injection) Marker interface for forecast-specific node communication.
 * Wired via TimeSeriesAnalyticsPlugin.
 */
public interface ForecastNodeCommunicator extends NodeCommunicator {}
