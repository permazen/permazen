
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JCompositeIndex;
import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.DeleteAction;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CompositeIndexTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final Permazen jdb = new Permazen(db, 2, null, this.getClasses());

        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            // Randomly create, delete, and modify objects, verifying expected index at each step
            final ArrayList<Top> tops = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                final Top top1 = this.getRandomTop(jtx);
                final Top top2 = this.getRandomTop(jtx);
                switch (top1 != null ? this.random.nextInt(4) : 0) {
                case 0:
                    if (this.random.nextBoolean())
                        jtx.create(Foo1.class);
                    else if (this.random.nextBoolean())
                        jtx.create(Foo2.class);
                    else
                        jtx.create(Foo3.class);
                    break;
                case 1:
                    jtx.delete(top1);
                    break;
                default:
                    switch (this.random.nextInt(4)) {
                    case 0:
                        top1.setRef1(top2);
                        break;
                    case 1:
                        top1.setRef2(top2);
                        break;
                    case 2:
                        top1.setInt(this.random.nextInt(20) - 10);
                        break;
                    case 3:
                        top1.setString(this.random.nextInt(5) != 0 ? "str" + this.random.nextInt(20) : null);
                        break;
                    default:
                        throw new RuntimeException("internal error");
                    }
                    break;
                }
                this.verifyIndexes(jtx);
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private Top getRandomTop(JTransaction jtx) {
        final NavigableSet<Top> tops = jtx.getAll(Top.class);
        final int num = tops.size();
        if (num == 0 || this.random.nextInt(10) > 7)
            return null;
        return (Top)tops.toArray()[this.random.nextInt(num)];
    }

    private void verifyIndexes(JTransaction jtx) {
        for (Class<?> startType : this.getClasses()) {
            for (Class<?> refType : this.getClasses()) {
                Assert.assertEquals(jtx.queryCompositeIndex(startType, "index1", refType, String.class).asSet(),
                  this.buildIndex1(jtx, refType, startType));
                Assert.assertEquals(jtx.queryCompositeIndex(startType, "index2", Integer.class, refType).asSet(),
                  this.buildIndex2(jtx, refType, startType));
            }
        }
    }

    private <R, T> Set<Tuple3<R, String, T>> buildIndex1(JTransaction jtx, Class<R> ref, Class<T> target) {
        final HashSet<Tuple3<R, String, T>> set = new HashSet<>();
        for (Top top : jtx.getAll(Top.class)) {
            try {
                set.add(new Tuple3<>(ref.cast(top.getRef1()), top.getString(), target.cast(top)));
            } catch (ClassCastException e) {
                continue;
            }
        }
        return set;
    }

    private <R, T> Set<Tuple3<Integer, R, T>> buildIndex2(JTransaction jtx, Class<R> ref, Class<T> target) {
        final HashSet<Tuple3<Integer, R, T>> set = new HashSet<>();
        for (Top top : jtx.getAll(Top.class)) {
            try {
                set.add(new Tuple3<>(top.getInt(), ref.cast(top.getRef2()), target.cast(top)));
            } catch (ClassCastException e) {
                continue;
            }
        }
        return set;
    }

    private ArrayList<Class<?>> getClasses() {
        final ArrayList<Class<?>> list = new ArrayList<>();
        list.add(Object.class);
        list.add(Top.class);
        list.add(Foo1.class);
        list.add(Foo2.class);
        list.add(Foo3.class);
        return list;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex2() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(IndexedOn2.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final NavigableMap<Tuple2<JObject, Thread.State>, NavigableSet<IndexedOn2>> view
              = jtx.queryCompositeIndex(IndexedOn2.class, "index2", JObject.class, Thread.State.class).asMap();

            final IndexedOn2 a = jtx.create(IndexedOn2.class);
            final IndexedOn2 b = jtx.create(IndexedOn2.class);
            final IndexedOn2 c = jtx.create(IndexedOn2.class);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, b, c));

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(null, Thread.State.RUNNABLE)),
              null);

            jtx.validate();

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, b, c));

            b.setField1(a);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, c));

            c.setField2(Thread.State.WAITING);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a));

            jtx.validate();

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex3() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(IndexedOn3.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final NavigableMap<Tuple3<JObject, Thread.State, Integer>, NavigableSet<IndexedOn3>> view
              = jtx.queryCompositeIndex(IndexedOn3.class, "index3", JObject.class, Thread.State.class, Integer.class).asMap();

            final IndexedOn3 a = jtx.create(IndexedOn3.class);
            final IndexedOn3 b = jtx.create(IndexedOn3.class);
            final IndexedOn3 c = jtx.create(IndexedOn3.class);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            a.setField3(-1234);
            b.setField3(-1234);
            c.setField3(-1234);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(a, b, c));

            jtx.validate();

            a.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(b, c));

            jtx.validate();

            b.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(c));

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              null);

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(a, b, c));

            a.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(b, c));

            jtx.validate();

            b.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(c));

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            c.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              null);

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            b.setField1(a);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.RUNNABLE, -1234)),
              null);

            Assert.assertEquals(view.get(new Tuple3<JObject, Thread.State, Integer>(null, Thread.State.RUNNABLE, 4567)),
              buildSet(c));

            jtx.validate();

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex4() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(IndexedOn4.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final NavigableMap<Tuple4<JObject, Thread.State, Integer, String>, NavigableSet<IndexedOn4>> view
              = jtx.queryCompositeIndex(IndexedOn4.class,
               "index4", JObject.class, Thread.State.class, Integer.class, String.class).asMap();

            final IndexedOn4 a = jtx.create(IndexedOn4.class);
            final IndexedOn4 b = jtx.create(IndexedOn4.class);
            final IndexedOn4 c = jtx.create(IndexedOn4.class);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            a.setField3(-1234);
            b.setField3(-1234);
            c.setField3(-1234);

            a.setField4("foobar");
            b.setField4("foobar");
            c.setField4("foobar");

            Assert.assertEquals(
              view.get(new Tuple4<JObject, Thread.State, Integer, String>(null, Thread.State.RUNNABLE, -1234, "foobar")),
              buildSet(a, b, c));

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            a.setField1(c);
            b.setField4("janfu");

            Assert.assertEquals(
              view.get(new Tuple4<JObject, Thread.State, Integer, String>(null, Thread.State.RUNNABLE, -1234, "foobar")),
              buildSet(c));

            jtx.validate();

            // Validate all of the unique exclusions

            a.setField1(null);
            b.setField1(null);
            c.setField1(null);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            a.setField3(-1234);
            b.setField3(-1234);
            c.setField3(-1234);

            a.setField4("foobar");
            b.setField4("foobar");
            c.setField4("foobar");

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            for (Thread.State s : new Thread.State[] {
              Thread.State.TIMED_WAITING, Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.NEW, Thread.State.TERMINATED }) {
                a.setField2(s);
                b.setField2(s);
                c.setField2(s);
                jtx.validate();
            }

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndexSubTypes() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(A.class, B.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final NavigableMap<Tuple2<JObject, Thread.State>, NavigableSet<IndexedOn2>> view
              = jtx.queryCompositeIndex(IndexedOn2.class, "index2", JObject.class, Thread.State.class).asMap();

            final NavigableMap<Tuple2<JObject, Thread.State>, NavigableSet<A>> viewA
              = jtx.queryCompositeIndex(A.class, "index2", JObject.class, Thread.State.class).asMap();

            final NavigableMap<Tuple2<JObject, Thread.State>, NavigableSet<B>> viewB
              = jtx.queryCompositeIndex(B.class, "index2", JObject.class, Thread.State.class).asMap();

            A a = jtx.create(A.class);
            B b = jtx.create(B.class);

            a.setField1(a);
            b.setField1(a);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<JObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(a, b));
            Assert.assertEquals(viewA.get(new Tuple2<JObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(a));
            Assert.assertEquals(viewB.get(new Tuple2<JObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(b));

            try {
                jtx.validate();
                assert false : "view = " + view;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            a.setField1(null);
            b.setField1(null);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeInitialUnique() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(C.class, D.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            C c = jtx.create(C.class);
            D d = jtx.create(D.class);

            try {
                jtx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            c.setField1(d);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JCompositeIndex(storageId = 11, name = "index1", fields = { "ref1", "string" })
    @JCompositeIndex(storageId = 12, name = "index2", fields = { "int", "ref2" })
    @PermazenType(storageId = 99)
    public abstract static class Top implements JObject {

        @JField(storageId = 1, onDelete = DeleteAction.UNREFERENCE)
        public abstract Top getRef1();
        public abstract void setRef1(Top ref1);

        @JField(storageId = 2, onDelete = DeleteAction.UNREFERENCE)
        public abstract Top getRef2();
        public abstract void setRef2(Top ref2);

        @JField(storageId = 3)
        public abstract int getInt();
        public abstract void setInt(int i);

        @JField(storageId = 4)
        public abstract String getString();
        public abstract void setString(String string);

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 10)
    public abstract static class Foo1 extends Top {
    }

    @PermazenType(storageId = 20)
    public abstract static class Foo2 extends Top {
    }

    @PermazenType(storageId = 30)
    public abstract static class Foo3 extends Top {
    }

// Model Classes #2

    public interface Fields extends JObject {

        JObject getField1();
        void setField1(JObject x);

        Thread.State getField2();
        void setField2(Thread.State x);

        int getField3();
        void setField3(int x);

        String getField4();
        void setField4(String x);
    }

    @JCompositeIndex(
      name = "index2",
      fields = { "field1", "field2" },
      unique = true,
      uniqueExclude = "null, TERMINATED")
    @PermazenType
    public interface IndexedOn2 extends Fields {
    }

    @JCompositeIndex(
      name = "index3",
      fields = { "field1", "field2", "field3" },
      unique = true,
      uniqueExclude = "null, TERMINATED, -1234")
    @PermazenType
    public interface IndexedOn3 extends Fields {
    }

    @JCompositeIndex(
      name = "index4",
      fields = { "field1", "field2", "field3", "field4" },
      unique = true,
      uniqueExclude = {
        "null, TIMED_WAITING, -1234, \"foobar\"",
        "null, BLOCKED, -1234, \"foobar\"",
        "null, WAITING, -1234, \"foobar\"",
        "null, NEW, -1234, \"foobar\"",
        "null, TERMINATED, -1234, \"foobar\""
      })
    @PermazenType
    public interface IndexedOn4 extends Fields {
    }

// Model Classes #3

    @PermazenType
    public abstract static class A implements IndexedOn2 {
    }

    @PermazenType
    public abstract static class B implements IndexedOn2 {
    }

// Model Classes #4

    @JCompositeIndex(name = "foo", fields = { "field1", "field2", "field3", "field4" }, unique = true)
    public interface Unique {
    }

    @PermazenType
    public abstract static class C implements Fields, Unique {
    }

    @PermazenType
    public abstract static class D implements Fields, Unique {
    }
}

