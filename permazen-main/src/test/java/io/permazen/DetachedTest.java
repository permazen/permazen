
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SetFieldChange;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.index.Index1;
import io.permazen.test.TestSupport;
import io.permazen.util.NavigableSets;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DetachedTest extends MainTestSupport {

    @Test(dataProvider = "shapshotCases")
    public void testDetached1(Class<? extends Person> personClass) throws Exception {

        final Permazen pdb = BasicTest.newPermazen(personClass);

        Person p1;
        Person p2;
        Person p3;

        Person detached;

        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.MANUAL);
        final PermazenTransaction stx = tx.getDetachedTransaction();
        PermazenTransaction.setCurrent(tx);
        try {

            p1 = tx.create(personClass);
            p2 = tx.create(personClass);
            p3 = tx.create(personClass);

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

            Assert.assertSame(p1, tx.get(p1.getObjId()));
            Assert.assertSame(p1a, stx.get(p1.getObjId()));

            TestSupport.checkSet(tx.getAll(personClass), buildSet(p1, p2, p3));
            TestSupport.checkSet(stx.getAll(personClass), buildSet(p1a));

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
            Assert.assertFalse(p2.isDetached());
            Person p2b = (Person)p2.copyOut();
            Assert.assertTrue(p2b.isDetached());
            Assert.assertSame(p2a, p2b);

            Assert.assertFalse(p1.isDetached());
            Person p1c = (Person)p1.copyOut("list", "map1.key", "map2.value");
            Assert.assertTrue(p1c.isDetached());
            Assert.assertSame(p1c, p1b);
            Assert.assertTrue(p1c.getMap1().keySet().iterator().next().exists());
            Assert.assertTrue(p1c.getMap2().values().iterator().next().exists());

            detached = p1a;

            tx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        Assert.assertTrue(detached.isDetached());
        detached.setName("Foobar");
        detached.setAge(19);
        detached.getSet().clear();

        final PermazenTransaction tx2 = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(tx2);
        try {

            // Reload objects
            p1 = tx2.get(p1);
            p2 = tx2.get(p2);
            p3 = tx2.get(p3);

            Assert.assertEquals(p1.getName(), "Person #1");
            Assert.assertEquals(p1.getAge(), 123);
            Assert.assertEquals(p1.getSet().size(), 1);

            Assert.assertTrue(detached.isDetached());
            Assert.assertFalse(p1.isDetached());

            detached.copyIn();

            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            p1.delete();
            Assert.assertFalse(p1.exists());

            detached.copyIn();
            Assert.assertTrue(p1.exists());

            Assert.assertTrue(p1.exists());
            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            p1.delete();
            Assert.assertFalse(p1.exists());
            Assert.assertFalse(p1.isDetached());

            detached.copyTo(tx2, -1, new CopyState());
            Assert.assertTrue(p1.exists());

            Assert.assertEquals(p1.getName(), "Foobar");
            Assert.assertEquals(p1.getAge(), 19);
            Assert.assertEquals(p1.getSet().size(), 0);

            p1.getSet().add(p1);
            p1.getSet().add(p2);
            detached.setName("Another Name");
            detached.setAge(123);
            detached.getSet().add(p2);
            detached.getSet().add(p3);
            detached.getMap1().clear();
            detached.getMap1().put(p1, 123123f);
            detached.getMap1().put(p3, null);
            detached.getMap2().clear();
            detached.getMap2().put(64f, p1);
            detached.getMap2().put(33.33f, p2);
            detached.getMap2().put(null, p3);

            detached.copyTo(tx2, -1, new CopyState());

            Assert.assertEquals(p1.getName(), "Another Name");
            Assert.assertEquals(p1.getAge(), 123);
            TestSupport.checkSet(p1.getSet(), buildSet(p2, p3));
            TestSupport.checkMap(p1.getMap1(), buildMap(p1, 123123f, p3, null));
            TestSupport.checkMap(p1.getMap2(), buildMap(64f, p1, 33.33f, p2, null, p3));
            if (p1 instanceof Person2)
                Assert.assertTrue(((Person2)p1).getFlag());

            tx2.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @DataProvider(name = "shapshotCases")
    public Object[][] genShapshotCases() {
        return new Object[][] {
            { Person.class  },
            { Person2.class }
        };
    }

    @Test
    public void testCopyRelated() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Foo.class);

        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.MANUAL);
        final PermazenTransaction stx = tx.getDetachedTransaction();
        PermazenTransaction.setCurrent(tx);
        try {

            final Foo f1 = tx.get(new ObjId("c811111111111111"), Foo.class);
            final Foo f2 = tx.get(new ObjId("c822222222222222"), Foo.class);
            final Foo f3 = tx.get(new ObjId("c833333333333333"), Foo.class);

            Assert.assertTrue(tx.recreate(f1));
            Assert.assertTrue(tx.recreate(f2));
            Assert.assertTrue(tx.recreate(f3));

            f1.setRef(f2);
            f2.setRef(f3);
            f3.setRef(f1);

            TestSupport.checkSet(f1.getReferrers(), buildSet(f3));
            TestSupport.checkSet(f2.getReferrers(), buildSet(f1));
            TestSupport.checkSet(f3.getReferrers(), buildSet(f2));

            tx.copyTo(stx, new CopyState(), f1.getWithRelatedObjects());
            final Foo f1s = stx.get(f1.getObjId(), Foo.class);

            final Foo f2s = (Foo)stx.get(f2.getObjId());
            final Foo f3s = (Foo)stx.get(f3.getObjId());

            Assert.assertTrue(f1s.exists());
            Assert.assertFalse(f2s.exists());
            Assert.assertTrue(f3s.exists());

            TestSupport.checkSet(f1s.getReferrers(), buildSet(f3s));
            TestSupport.checkSet(f2s.getReferrers(), buildSet(f1s));
            TestSupport.checkSet(f3s.getReferrers(), buildSet());

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDanglingReference() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Foo.class, Foo2.class);

        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.MANUAL);
        final PermazenTransaction stx = tx.getDetachedTransaction();
        PermazenTransaction.setCurrent(tx);
        try {

            final Foo f1 = tx.create(Foo2.class);
            final Foo f2 = tx.create(Foo2.class);

            f1.setRef(f2);
            f2.delete();

            try {
                f1.copyOut();
            } catch (DeletedObjectException e) {
                assert false;
            }

            try {
                f2.copyOut();
                assert false;
            } catch (DeletedObjectException e) {
                // expected
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @PermazenSetField(storageId = 103, element = @PermazenField(storageId = 104, forwardCascades = "set"))
        public abstract Set<Person> getSet();

        @PermazenListField(storageId = 105, element = @PermazenField(storageId = 106, forwardCascades = "list"))
        public abstract List<Person> getList();

        @PermazenMapField(storageId = 107,
          key = @PermazenField(storageId = 108, forwardCascades = "map1.key"),
          value = @PermazenField(storageId = 109))
        public abstract Map<Person, Float> getMap1();

        @PermazenMapField(storageId = 110,
          key = @PermazenField(storageId = 111),
          value = @PermazenField(storageId = 112, forwardCascades = "map2.value"))
        public abstract Map<Float, Person> getMap2();
    }

    @PermazenType(storageId = 150)
    public abstract static class Person2 extends Person {

        private boolean flag;

        public boolean getFlag() {
            return this.flag;
        }

        // @OnChange forces registration of change listener, and slower field-by-field copy
        @OnChange("set")
        private void onSetChange(SetFieldChange<Person2> change) {
            this.flag = true;
        }
    }

    @PermazenType(storageId = 200)
    public abstract static class Foo implements PermazenObject {

        @PermazenField(storageId = 201, inverseDelete = DeleteAction.IGNORE, allowDeleted = true)
        public abstract Foo getRef();
        public abstract void setRef(Foo ref);

        public NavigableSet<Foo> getReferrers() {
            final NavigableSet<Foo> referrers = this.queryFoo().asMap().get(this);
            return referrers != null ? referrers : NavigableSets.<Foo>empty();
        }

        public Index1<Foo, Foo> queryFoo() {
            return this.getTransaction().querySimpleIndex(Foo.class, "ref", Foo.class);
        }

        public Stream<Foo> getWithRelatedObjects() {
            return Stream.concat(Stream.of(this), this.getReferrers().stream());
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 300)
    public abstract static class Foo2 extends Foo {

        @Override
        public Stream<Foo> getWithRelatedObjects() {
            return Arrays.<Foo>asList(this, this.getRef()).stream();
        }
    }
}
