/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import java.time.Duration;

public class TimeSeriesSettings {

    // ======================================
    // Model parameters
    // ======================================
    public static final int DEFAULT_SHINGLE_SIZE = 8;

    // max shingle size we have seen from external users
    // the larger shingle size, the harder to fill in a complete shingle
    public static final int MAX_SHINGLE_SIZE = 60;

    public static final String CONFIG_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";

    public static final String JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";

    // 100,000 insertions costs roughly 1KB.
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    public static final double DOOR_KEEPER_FALSE_POSITIVE_RATE = 0.01;

    // clean up door keeper every 60 intervals
    public static final int DOOR_KEEPER_MAINTENANCE_FREQ = 60;

    // 1 million insertion costs roughly 1 MB.
    public static final int DOOR_KEEPER_FOR_CACHE_MAX_INSERTION = 1_000_000;

    // for a real-time operation, we trade off speed for memory as real time opearation
    // only has to do one update/scoring per interval
    public static final double REAL_TIME_BOUNDING_BOX_CACHE_RATIO = 0;

    // ======================================
    // Historical analysis
    // ======================================
    public static final int MAX_BATCH_TASK_PIECE_SIZE = 10_000;

    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    public static final float INTERVAL_RATIO_FOR_REQUESTS = 0.9f;

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    // the size of the buffer used for rcf deserialization
    public static final int SERIALIZATION_BUFFER_BYTES = 512;

    // Sets the cap on the number of buffer that can be allocated by the rcf deserialization
    // buffer pool. Each buffer is of 512 bytes. Memory occupied by 20 buffers is 10.24 KB.
    public static final int MAX_TOTAL_RCF_SERIALIZATION_BUFFERS = 20;
}
