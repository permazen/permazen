
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.Test;

public class GetModelClassTest extends TestSupport {

    @Test
    public void testGetModelClass() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(ModelA.class, ModelB.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final ModelA a = jtx.create(ModelA.class);
            final ModelB b = jtx.create(ModelB.class);

            Assert.assertEquals(a.getModelClass(), ModelA.class);
            Assert.assertEquals(b.getModelClass(), ModelB.class);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class ModelA implements JObject {
    }

    @PermazenType
    public interface ModelB extends JObject {
    }
}
