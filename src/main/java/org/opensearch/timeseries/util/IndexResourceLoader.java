/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.timeseries.util;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Loads index-related resources such as mappings and scripts from the classpath.
 * This exists to let callers fetch mapping/script files without depending on
 * IndexManagement (meant for single-tenant index management), keeping
 * non-index-management code decoupled from that class.
 * The helpers are purely classpath readers and work the same in single-tenant
 * or multi-tenant deployments.
 */
public final class IndexResourceLoader {

    private IndexResourceLoader() {}

    /**
     * Retrieve mapping content from a classpath resource.
     *
     * @param mappingFileRelativePath relative path to the mapping file
     * @return file content as a string
     * @throws IOException if the file cannot be read
     */
    public static String getMappings(String mappingFileRelativePath) throws IOException {
        URL url = IndexResourceLoader.class.getClassLoader().getResource(mappingFileRelativePath);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Retrieve script content from a classpath resource.
     *
     * @param scriptFileRelativePath relative path to the script file
     * @return file content as a string
     * @throws IOException if the file cannot be read
     */
    public static String getScripts(String scriptFileRelativePath) throws IOException {
        URL url = IndexResourceLoader.class.getClassLoader().getResource(scriptFileRelativePath);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Retrieve flattened result mapping content with dynamic fields allowed.
     *
     * @param mappingFileRelativePath relative path to the mapping file
     * @return mapping content as a string with "dynamic" set to true
     * @throws IOException if the file cannot be read
     */
    public static String getFlattenedResultMappings(String mappingFileRelativePath) throws IOException {
        URL url = IndexResourceLoader.class.getClassLoader().getResource(mappingFileRelativePath);
        String mappingJson = Resources.toString(url, Charsets.UTF_8);
        return getFlattenedResultMappingsFromContent(mappingJson);
    }

    /**
     * Retrieve flattened result mapping content from JSON text with dynamic fields allowed.
     *
     * @param mappingJson mapping content as JSON
     * @return mapping content as a string with "dynamic" set to true
     * @throws IOException if the content cannot be parsed
     */
    public static String getFlattenedResultMappingsFromContent(String mappingJson) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> mapping = objectMapper.readValue(mappingJson, Map.class);
        mapping.put("dynamic", true);
        return objectMapper.writeValueAsString(mapping);
    }

    /**
     * Get config index mapping in json format.
     *
     * @return config index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getConfigMappings() throws IOException {
        return getMappings(TimeSeriesSettings.CONFIG_INDEX_MAPPING_FILE);
    }

    /**
     * Get job index mapping in json format.
     *
     * @return job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getJobMappings() throws IOException {
        return getMappings(TimeSeriesSettings.JOBS_INDEX_MAPPING_FILE);
    }
}
