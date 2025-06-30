/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.Objects;
import java.util.function.Supplier;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Shared helper for loading factory implementations from class-name settings.
 */
public final class ConfiguredFactoryLoader {
    private ConfiguredFactoryLoader() {}

    public static <T> T load(
        Settings settings,
        Setting<String> classSetting,
        String description,
        Class<T> factoryType,
        Class<? extends T> defaultFactoryClass,
        Supplier<? extends T> defaultFactorySupplier,
        ClassLoader classLoader
    ) {
        Objects.requireNonNull(settings, "settings must not be null");
        Objects.requireNonNull(classSetting, "classSetting must not be null");
        Objects.requireNonNull(description, "description must not be null");
        Objects.requireNonNull(factoryType, "factoryType must not be null");
        Objects.requireNonNull(defaultFactoryClass, "defaultFactoryClass must not be null");
        Objects.requireNonNull(defaultFactorySupplier, "defaultFactorySupplier must not be null");
        Objects.requireNonNull(classLoader, "classLoader must not be null");

        String className = classSetting.get(settings);
        if (className == null || className.isEmpty()) {
            return defaultFactorySupplier.get();
        }

        try {
            Class<?> factoryClass = Class.forName(className, true, classLoader);
            if (!factoryType.isAssignableFrom(factoryClass)) {
                throw new IllegalStateException(className + " must implement " + factoryType.getSimpleName());
            }
            if (factoryClass == defaultFactoryClass) {
                return defaultFactorySupplier.get();
            }
            return factoryType.cast(factoryClass.getConstructor().newInstance());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load " + description + ": " + className, e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate " + description + ": " + className, e);
        }
    }
}
