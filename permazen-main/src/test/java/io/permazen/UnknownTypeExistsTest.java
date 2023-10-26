
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnknownTypeExistsTest extends TestSupport {

    @Test
    public void testUnknownTypeExists() throws Exception {

        final Database db = new Database(new SimpleKVDatabase());
        final PermazenFactory factory1 = new PermazenFactory();
        factory1.setDatabase(db);
        factory1.setSchemaVersion(1);
        factory1.setModelClasses(Foo.class);
        final Permazen jdb1 = factory1.newPermazen();

        JTransaction jtx;

        jtx = jdb1.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        final Foo foo1;
        final Foo foo2;
        try {

            foo1 = jtx.create(Foo.class);
            foo2 = jtx.create(Foo.class);

            Assert.assertTrue(foo1.exists());
            Assert.assertTrue(foo2.exists());

            foo2.delete();

            Assert.assertTrue(foo1.exists());
            Assert.assertFalse(foo2.exists());

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        final PermazenFactory factory2 = new PermazenFactory();
        factory2.setDatabase(db);
        factory2.setSchemaVersion(2);
        factory2.setModelClasses(Bar.class);
        final Permazen jdb2 = factory2.newPermazen();

        jtx = jdb2.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        final Bar bar;
        try {

            bar = jtx.create(Bar.class);

            final JObject foo1x = jtx.get(foo1.getObjId());
            final JObject foo2x = jtx.get(foo2.getObjId());

            Assert.assertTrue(foo1x instanceof UntypedJObject);
            Assert.assertTrue(foo2x instanceof UntypedJObject);

            Assert.assertTrue(foo1x.exists());
            Assert.assertFalse(foo2x.exists());
            Assert.assertTrue(bar.exists());

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Foo implements JObject {
    }

    @PermazenType
    public abstract static class Bar implements JObject {
    }
}
