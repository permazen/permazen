
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UntypedJObjectTest extends TestSupport {

    @Test
    public void testUntypedJObject() throws Exception {

    // Create a Foo and Bar in schema version 1

        final Database db = new Database(new SimpleKVDatabase());

        final ObjId fooId;
        final ObjId barId;

        final JSimpleDBFactory factory1 = new JSimpleDBFactory();
        factory1.setDatabase(db);
        factory1.setSchemaVersion(1);
        factory1.setModelClasses(Foo.class, Bar.class);
        final JSimpleDB jdb1 = factory1.newJSimpleDB();
        final JTransaction jtx1 = jdb1.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx1);
        try {

            final Foo foo = jtx1.create(Foo.class);
            foo.setName("foo");
            final Bar bar = jtx1.create(Bar.class);
            bar.setName("bar");

            foo.setObject(bar);
            foo.setHasName(bar);

            fooId = foo.getObjId();
            barId = bar.getObjId();

            jtx1.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

    // Query getAll() in schema version 2 transaction

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

            final Foo foo = (Foo)jobjs.iterator().next();
            final JObject bar = nobjs.iterator().next();

            Assert.assertTrue(foo instanceof Foo);
            Assert.assertTrue(bar instanceof UntypedJObject);

            Assert.assertEquals(foo.getObjId(), fooId);
            Assert.assertEquals(bar.getObjId(), barId);

            Assert.assertEquals(foo.getObject(), bar);  // this foo -> bar reference is preserved even though bar is now untyped
            Assert.assertNull(foo.getHasName());        // this foo -> bar reference is cleared because bar is no longer a HasName

            jtx2.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

    // Query HasName index in schema version 2 transaction

        final JTransaction jtx3 = jdb2.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx3);
        try {

            final NavigableMap<String, NavigableSet<Foo>> fobjs = jtx3.queryIndex(Foo.class, "name", String.class).asMap();
            final NavigableMap<String, NavigableSet<HasName>> hobjs = jtx3.queryIndex(HasName.class, "name", String.class).asMap();
            final NavigableMap<String, NavigableSet<JObject>> jobjs = jtx3.queryIndex(JObject.class, "name", String.class).asMap();

            final Foo foo = jtx3.get(fooId, Foo.class);
            final JObject bar = jtx3.get(barId);

            Assert.assertTrue(foo.exists());
            Assert.assertTrue(bar.exists());
            Assert.assertTrue(bar instanceof UntypedJObject);

            TestSupport.checkMap(fobjs, buildMap(
              "foo",     buildSet(foo)));

            TestSupport.checkMap(hobjs, buildMap(
              "foo",     buildSet(foo)));

            TestSupport.checkMap(jobjs, buildMap(
              "foo",     buildSet(foo),
              "bar",     buildSet(bar)));

            jtx3.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

    }

// Model Classes

    public interface HasName extends JObject {

        @JField(indexed = true)
        String getName();
        void setName(String name);
    }

    @JSimpleClass
    public abstract static class Foo implements HasName {
        // this class exists in schema versions 1 and 2

        public abstract Object getObject();
        public abstract void setObject(Object obj);

        public abstract HasName getHasName();
        public abstract void setHasName(HasName obj);
    }

    @JSimpleClass
    public abstract static class Bar implements HasName {
        // this class exists in schema versions 1 only
    }
}

