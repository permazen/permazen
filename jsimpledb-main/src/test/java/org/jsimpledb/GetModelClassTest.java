
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetModelClassTest extends TestSupport {

    @Test
    public void testGetModelClass() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(ModelA.class, ModelB.class);
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

    @JSimpleClass
    public abstract static class ModelA implements JObject {
    }

    @JSimpleClass
    public interface ModelB extends JObject {
    }
}
