/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.constant.CommonName;

/**
 * A container for dynamic string settings, specialized for managing node roles
 * for the time series plugin.
 */
public class DynamicStringSetting {
    private static final Logger logger = LogManager.getLogger(DynamicStringSetting.class);

    private static DynamicStringSetting INSTANCE;

    // Role names
    public static final String CLOUDMAP_WATCHER_ROLE = "cloudmap_watcher";
    public static final String COORDINATOR_ROLE = "coordinator";
    public static final String MODEL_ROLE = "model";

    private static final Set<String> VALID_ROLES = Set.of(CLOUDMAP_WATCHER_ROLE, COORDINATOR_ROLE, MODEL_ROLE);

    public static final Setting<List<String>> NODE_ROLES_SETTING = Setting
        .listSetting(
            CommonName.SETTING_PREFIX + "node.roles",
            List.of(), // Default to all roles
            s -> s, // parser
            roles -> { // validator
                if (roles.isEmpty()) {
                    throw new IllegalArgumentException("Node roles cannot be empty.");
                }
                if (new HashSet<>(roles).size() != roles.size()) {
                    throw new IllegalArgumentException("Duplicate roles found: " + roles);
                }
                for (String role : roles) {
                    if (!VALID_ROLES.contains(role)) {
                        throw new IllegalArgumentException("Invalid role: " + role + ". Valid roles are " + VALID_ROLES);
                    }
                }
            },
            Property.NodeScope,
            Property.Dynamic
        );

    private ClusterService clusterService;
    /** Latest setting value for each registered key. Thread-safe is required. */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    private final Map<String, Setting<?>> settings;

    private DynamicStringSetting() {
        this.settings = Map.of(NODE_ROLES_SETTING.getKey(), NODE_ROLES_SETTING);
    }

    public static DynamicStringSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DynamicStringSetting();
        }
        return INSTANCE;
    }

    private void setSettingsUpdateConsumers() {
        for (Setting<?> setting : settings.values()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(setting, newVal -> {
                logger.info("The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                latestSettings.put(setting.getKey(), newVal);
            });
        }
    }

    public void init(ClusterService clusterService) {
        this.clusterService = clusterService;
        setSettingsUpdateConsumers();
    }

    @SuppressWarnings("unchecked")
    private List<String> getRoles() {
        return (List<String>) latestSettings.getOrDefault(NODE_ROLES_SETTING.getKey(), NODE_ROLES_SETTING.getDefault(Settings.EMPTY));
    }

    public boolean isCloudmapWatcherNode() {
        return getRoles().contains(CLOUDMAP_WATCHER_ROLE);
    }

    public boolean isCoordinatorNode() {
        return getRoles().contains(COORDINATOR_ROLE);
    }

    public boolean isModelNode() {
        return getRoles().contains(MODEL_ROLE);
    }

    public List<Setting<?>> getSettings() {
        return List.of(NODE_ROLES_SETTING);
    }
}
