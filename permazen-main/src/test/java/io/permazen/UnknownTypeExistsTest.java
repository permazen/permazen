
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.kv.simple.MemoryKVDatabase;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnknownTypeExistsTest extends MainTestSupport {

    @Test
    public void testUnknownTypeExists() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final Permazen jdb1 = PermazenConfig.builder()
          .database(db)
          .modelClasses(Foo.class)
          .build()
          .newPermazen();

        JTransaction jtx;

        jtx = jdb1.createTransaction(ValidationMode.AUTOMATIC);
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

        final Permazen jdb2 = PermazenConfig.builder()
          .database(db)
          .modelClasses(Bar.class)
          .build()
          .newPermazen();

        jtx = jdb2.createTransaction(ValidationMode.AUTOMATIC);
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
