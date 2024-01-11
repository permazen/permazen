
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

        PermazenTransaction ptx;

        ptx = jdb1.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        final Foo foo1;
        final Foo foo2;
        try {

            foo1 = ptx.create(Foo.class);
            foo2 = ptx.create(Foo.class);

            Assert.assertTrue(foo1.exists());
            Assert.assertTrue(foo2.exists());

            foo2.delete();

            Assert.assertTrue(foo1.exists());
            Assert.assertFalse(foo2.exists());

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }

        final Permazen jdb2 = PermazenConfig.builder()
          .database(db)
          .modelClasses(Bar.class)
          .build()
          .newPermazen();

        ptx = jdb2.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        final Bar bar;
        try {

            bar = ptx.create(Bar.class);

            final PermazenObject foo1x = ptx.get(foo1.getObjId());
            final PermazenObject foo2x = ptx.get(foo2.getObjId());

            Assert.assertTrue(foo1x instanceof UntypedPermazenObject);
            Assert.assertTrue(foo2x instanceof UntypedPermazenObject);

            Assert.assertTrue(foo1x.exists());
            Assert.assertFalse(foo2x.exists());
            Assert.assertTrue(bar.exists());

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Foo implements PermazenObject {
    }

    @PermazenType
    public abstract static class Bar implements PermazenObject {
    }
}
