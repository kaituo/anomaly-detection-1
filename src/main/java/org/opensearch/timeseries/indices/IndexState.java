/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.indices;

/**
 * Tracks per-index mapping/setting update state and schema version derived from mapping files.
 */
public class IndexState {
    // keep track of whether the mapping version is up-to-date
    public Boolean mappingUpToDate;
    // keep track of whether the setting needs to change
    public Boolean settingUpToDate;
    // record schema version reading from the mapping file
    public Integer schemaVersion;

    public IndexState(String mappingFile) {
        this(mappingFile, false, false);
    }

    public IndexState(String mappingFile, boolean mappingUpToDate, boolean settingUpToDate) {
        this.mappingUpToDate = mappingUpToDate;
        this.settingUpToDate = settingUpToDate;
        this.schemaVersion = IndexManagement.parseSchemaVersion(mappingFile);
    }
}
