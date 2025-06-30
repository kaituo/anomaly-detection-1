/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;

/**
 * Plugin-owned view of field mappings used by time series validation code.
 *
 * <p>The background for this type is that {@link GetFieldMappingsResponse} is a core
 * OpenSearch transport response whose map-based constructor is package-private. The
 * single-tenant path can obtain that response directly from the transport client, but the
 * SDK-backed multi-tenant path cannot. In the SDK path we build field mapping data from
 * REST responses: local indices use the field mapping API and remote indices use field caps,
 * then normalize both into the same logical shape. Before this class existed,
 * {@code SdkDataAccess} had to instantiate {@code GetFieldMappingsResponse} reflectively in
 * order to hand synthesized mappings back to shared validation code.
 *
 * <p>That reflective construction is both brittle and blocked by forbidden APIs checks.
 * The validation layer does not need the full transport response behavior; it only reads the
 * mapping structure, each field's full name, and the mapping source as a map. This DTO keeps
 * that narrow contract inside the plugin, lets transport-backed code adapt native
 * {@code GetFieldMappingsResponse} instances into the same view, and avoids coupling plugin
 * logic to OpenSearch core constructor visibility.
 */
public class FieldMappingsView {

    private final Map<String, Map<String, FieldMapping>> mappings;

    public FieldMappingsView(Map<String, Map<String, FieldMapping>> mappings) {
        Objects.requireNonNull(mappings, "mappings must not be null");

        Map<String, Map<String, FieldMapping>> immutableMappings = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, FieldMapping>> indexEntry : mappings.entrySet()) {
            Map<String, FieldMapping> fieldMappings = indexEntry.getValue();
            immutableMappings
                .put(
                    indexEntry.getKey(),
                    Collections.unmodifiableMap(new LinkedHashMap<>(fieldMappings == null ? Collections.emptyMap() : fieldMappings))
                );
        }
        this.mappings = Collections.unmodifiableMap(immutableMappings);
    }

    public static FieldMappingsView from(GetFieldMappingsResponse response) {
        Objects.requireNonNull(response, "response must not be null");

        Map<String, Map<String, FieldMapping>> mappings = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> indexEntry : response.mappings().entrySet()) {
            Map<String, FieldMapping> fieldMappings = new LinkedHashMap<>();
            for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetadata> fieldEntry : indexEntry.getValue().entrySet()) {
                GetFieldMappingsResponse.FieldMappingMetadata metadata = fieldEntry.getValue();
                fieldMappings.put(fieldEntry.getKey(), metadata == null ? null : FieldMapping.from(metadata));
            }
            mappings.put(indexEntry.getKey(), fieldMappings);
        }
        return new FieldMappingsView(mappings);
    }

    public Map<String, Map<String, FieldMapping>> mappings() {
        return mappings;
    }

    public static class FieldMapping {
        private final String fullName;
        private final Map<String, Object> source;

        public FieldMapping(String fullName, Map<String, Object> source) {
            this.fullName = Objects.requireNonNull(fullName, "fullName must not be null");
            this.source = Collections.unmodifiableMap(new LinkedHashMap<>(source == null ? Collections.emptyMap() : source));
        }

        public static FieldMapping from(GetFieldMappingsResponse.FieldMappingMetadata metadata) {
            Objects.requireNonNull(metadata, "metadata must not be null");
            return new FieldMapping(metadata.fullName(), metadata.sourceAsMap());
        }

        public String fullName() {
            return fullName;
        }

        public Map<String, Object> sourceAsMap() {
            return source;
        }
    }
}
