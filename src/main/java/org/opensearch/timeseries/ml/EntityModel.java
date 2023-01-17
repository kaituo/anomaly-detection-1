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

package org.opensearch.timeseries.ml;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import org.opensearch.timeseries.model.Entity;


public class EntityModel<T> {
    private Entity entity;
    private Deque<Sample> samples;
    private T model;

    public EntityModel(Entity entity, Deque<Sample> samples, T model) {
        this.entity = entity;
        this.samples = samples;
        this.model = model;
    }

    /**
     * In old checkpoint mapping, we don't have entity. It's fine we are missing
     * entity as it is mostly used for debugging.
     * @return entity
     */
    public Optional<Entity> getEntity() {
        return Optional.ofNullable(entity);
    }

    public Deque<Sample> getSamples() {
        return this.samples;
    }

    public void addSample(Sample sample) {
        if (this.samples == null) {
            this.samples = new ArrayDeque<>();
        }
        if (sample != null && sample.getValueList() != null && sample.getValueList().length != 0) {
            this.samples.add(sample);
        }
    }

    /**
     * Sets a model.
     *
     * @param model model instance
     */
    public void setModel(T model) {
        this.model = model;
    }

    /**
     *
     * @return optional model.
     */
    public Optional<T> getModel() {
        return Optional.ofNullable(this.model);
    }

    public void clearSamples() {
        if (samples != null) {
            samples.clear();
        }
    }

    public void clear() {
        clearSamples();
        model = null;
    }
}
