
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.NavigableSet;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.JTransient;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UntypedJObjectTest extends TestSupport {

    @Test
    public void testUntypedJObject() throws Exception {

        final Database db = new Database(new SimpleKVDatabase());

        final Foo foo;
        final Bar bar;

        final JSimpleDBFactory factory1 = new JSimpleDBFactory();
        factory1.setDatabase(db);
        factory1.setSchemaVersion(1);
        factory1.setModelClasses(Foo.class, Bar.class);
        final JSimpleDB jdb1 = factory1.newJSimpleDB();
        final JTransaction jtx1 = jdb1.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx1);
        try {

            foo = jtx1.create(Foo.class);
            bar = jtx1.create(Bar.class);

            jtx1.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        final JSimpleDBFactory factory2 = new JSimpleDBFactory();
        factory2.setDatabase(db);
        factory2.setSchemaVersion(2);
        factory2.setModelClasses(Foo.class);
        final JSimpleDB jdb2 = factory2.newJSimpleDB();
        final JTransaction jtx2 = jdb2.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx2);
        try {

            final NavigableSet<JObject> jobjs = jtx2.getAll(JObject.class);
            final NavigableSet<UntypedJObject> nobjs = jtx2.getAll(UntypedJObject.class);

            Assert.assertEquals(jobjs.size(), 1);
            Assert.assertEquals(nobjs.size(), 1);

            Assert.assertEquals(jobjs.iterator().next().getObjId(), foo.getObjId());
            Assert.assertEquals(nobjs.iterator().next().getObjId(), bar.getObjId());

            jtx2.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Foo implements JObject {
        // this class exists in schema versions 1 and 2
    }

    @JSimpleClass
    public abstract static class Bar implements JObject {
        // this class exists in schema versions 1 only
    }
}

