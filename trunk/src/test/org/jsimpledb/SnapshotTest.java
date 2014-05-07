
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SnapshotTest extends TestSupport {

    @Test
    public void testSnapshot1() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);

        Person p1;
        Person p2;
        Person p3;

        Person snapshot;

        final JTransaction tx = jdb.createTransaction(true, ValidationMode.MANUAL);
        final JTransaction stx = tx.getSnapshotTransaction();
        JTransaction.setCurrent(tx);
        try {

            p1 = tx.create(Person.class);
            p2 = tx.create(Person.class);
            p3 = tx.create(Person.class);

            p1.setName("Person #1");
            p1.setAge(123);
            p1.getSet().add(p2);
            p1.getList().add(p3);
            p1.getMap1().put(p1, 123.45f);
            p1.getMap1().put(p2, 456.78f);
            p1.getMap2().put(11.11f, p1);
            p1.getMap2().put(22.22f, p2);
            p1.getMap2().put(33.33f, p3);

            Person p1a = (Person)p1.copyOut();
            Assert.assertFalse(p1a == p1);
            Assert.assertFalse(p1a.equals(p1));
            Assert.assertEquals(p1a.getName(), "Person #1");
            Assert.assertEquals(p1a.getAge(), 123);
            Assert.assertEquals(p1a.getSet().size(), 1);
            Assert.assertEquals(p1a.getList().size(), 1);
            Assert.assertEquals(p1a.getMap1().size(), 2);

            Assert.assertSame(p1, tx.getJObject(p1.getObjId()));
            Assert.assertSame(p1, tx.getJSimpleDB().getJObject(p1.getObjId()));
            Assert.assertSame(p1a, stx.getJObject(p1.getObjId()));

            Assert.assertEquals(tx.getAll(Person.class), buildSet(p1, p2, p3));
            Assert.assertEquals(stx.getAll(Person.class), buildSet(p1a));

            Person p2a = p1a.getSet().iterator().next();

            Assert.assertTrue(p1a.exists());
            Assert.assertFalse(p2a.exists());

            p2.getSet().add(p2);
            p2.getList().add(p2);
            p2.getMap1().put(p2, 0.0f);
            p2.getMap2().put(0.0f, p2);

            Person p1b = (Person)p1.copyOut("set");
            Assert.assertSame(p1a, p1b);
            Assert.assertTrue(p1b.exists());

            Assert.assertSame(p1a.getSet().iterator().next(), p2a);
            Person p2b = (Person)p2.copyOut();
            Assert.assertSame(p2a, p2b);

            Person p1c = (Person)p1.copyOut("list.element", "map1", "map2.value");
            Assert.assertSame(p1c, p1b);
            Assert.assertTrue(p1c.getMap1().keySet().iterator().next().exists());
            Assert.assertTrue(p1c.getMap2().values().iterator().next().exists());

            snapshot = p1a;

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        snapshot.setName("Foobar");
        snapshot.setAge(19);
        snapshot.getSet().clear();

        final JTransaction tx2 = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(tx2);
        try {

            Assert.assertEquals(p1.getName(), "Person #1");
            Assert.assertEquals(p1.getAge(), 123);
            Assert.assertEquals(p1.getSet().size(), 1);

            snapshot.copyIn();

            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            p1.delete();
            Assert.assertFalse(p1.exists());

            snapshot.copyIn();
            Assert.assertTrue(p1.exists());

            Assert.assertTrue(p1.exists());
            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            p1.delete();
            Assert.assertFalse(p1.exists());

            snapshot.copyTo(tx2);
            Assert.assertTrue(p1.exists());

            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            tx2.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testSnapshotInvalid() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(tx);
        try {

            Person p1 = tx.create(Person.class);

            try {
                p1.copyOut("age");
                assert false;
            } catch (IllegalArgumentException e) {
                this.log.info("got expected " + e);
            }

            try {
                p1.copyOut("set.foo");
                assert false;
            } catch (IllegalArgumentException e) {
                this.log.info("got expected " + e);
            }

            try {
                p1.copyOut("map1.value");
                assert false;
            } catch (IllegalArgumentException e) {
                this.log.info("got expected " + e);
            }

            try {
                p1.copyOut("map2.key");
                assert false;
            } catch (IllegalArgumentException e) {
                this.log.info("got expected " + e);
            }

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        @JField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @JField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @JSetField(storageId = 103, element = @JField(storageId = 104))
        public abstract Set<Person> getSet();

        @JListField(storageId = 105, element = @JField(storageId = 106))
        public abstract List<Person> getList();

        @JMapField(storageId = 107,
          key = @JField(storageId = 108),
          value = @JField(storageId = 109))
        public abstract Map<Person, Float> getMap1();

        @JMapField(storageId = 110,
          key = @JField(storageId = 111),
          value = @JField(storageId = 112))
        public abstract Map<Float, Person> getMap2();
    }
}

