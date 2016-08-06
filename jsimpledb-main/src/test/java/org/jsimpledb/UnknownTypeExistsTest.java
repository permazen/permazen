
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.JTransient;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnknownTypeExistsTest extends TestSupport {

    @Test
    public void testUnknownTypeExists() throws Exception {

        final Database db = new Database(new SimpleKVDatabase());
        final JSimpleDBFactory factory1 = new JSimpleDBFactory();
        factory1.setDatabase(db);
        factory1.setSchemaVersion(1);
        factory1.setModelClasses(Foo.class);
        final JSimpleDB jdb1 = factory1.newJSimpleDB();

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

        final JSimpleDBFactory factory2 = new JSimpleDBFactory();
        factory2.setDatabase(db);
        factory2.setSchemaVersion(2);
        factory2.setModelClasses(Bar.class);
        final JSimpleDB jdb2 = factory2.newJSimpleDB();

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

    @JSimpleClass
    public abstract static class Foo implements JObject {
    }

    @JSimpleClass
    public abstract static class Bar implements JObject {
    }
}

