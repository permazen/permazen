
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;

import org.testng.Assert;
import org.testng.annotations.Test;

public class GetModelClassTest extends MainTestSupport {

    @Test
    public void testGetModelClass() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(ModelA.class, ModelB.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            final ModelA a = ptx.create(ModelA.class);
            final ModelB b = ptx.create(ModelB.class);

            Assert.assertEquals(a.getModelClass(), ModelA.class);
            Assert.assertEquals(b.getModelClass(), ModelB.class);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class ModelA implements PermazenObject {
    }

    @PermazenType
    public interface ModelB extends PermazenObject {
    }
}
