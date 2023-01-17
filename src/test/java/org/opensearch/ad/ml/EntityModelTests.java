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

package org.opensearch.ad.ml;

import java.util.ArrayDeque;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.ml.createFromValueOnlySamples;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class EntityModelTests extends OpenSearchTestCase {

    private ThresholdedRandomCutForest trcf;

    @Before
    public void setup() {
        this.trcf = new ThresholdedRandomCutForest(ThresholdedRandomCutForest.builder().dimensions(2).internalShinglingEnabled(true));
    }

    public void testNullInternalSampleQueue() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> model = new createFromValueOnlySamples<>(null, null, null);
        model.addValueOnlySample(new double[] { 0.8 });
        assertEquals(1, model.getValueOnlySamples().size());
    }

    public void testNullInputSample() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> model = new createFromValueOnlySamples<>(null, null, null);
        model.addValueOnlySample(null);
        assertEquals(0, model.getValueOnlySamples().size());
    }

    public void testEmptyInputSample() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> model = new createFromValueOnlySamples<>(null, null, null);
        model.addValueOnlySample(new double[] {});
        assertEquals(0, model.getValueOnlySamples().size());
    }

    @Test
    public void trcf_constructor() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> em = new createFromValueOnlySamples<>(null, new ArrayDeque<>(), trcf);
        assertEquals(trcf, em.getModel().get());
    }

    @Test
    public void clear() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> em = new createFromValueOnlySamples<>(null, new ArrayDeque<>(), trcf);

        em.clear();

        assertTrue(em.getValueOnlySamples().isEmpty());
        assertFalse(em.getModel().isPresent());
    }

    @Test
    public void setTrcf() {
        createFromValueOnlySamples<ThresholdedRandomCutForest> em = new createFromValueOnlySamples<>(null, null, null);
        assertFalse(em.getModel().isPresent());

        em.setModel(this.trcf);
        assertTrue(em.getModel().isPresent());
    }
}
