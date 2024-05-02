
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenCompositeIndex;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.annotation.Values;
import io.permazen.annotation.ValuesList;
import io.permazen.core.Database;
import io.permazen.core.DeleteAction;
import io.permazen.kv.simple.MemoryKVDatabase;
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

public class CompositeIndexTest extends MainTestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final Permazen pdb = BasicTest.newPermazen(db, this.getClasses());

        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            // Randomly create, delete, and modify objects, verifying expected index at each step
            final ArrayList<Top> tops = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                final Top top1 = this.getRandomTop(ptx);
                final Top top2 = this.getRandomTop(ptx);
                switch (top1 != null ? this.random.nextInt(4) : 0) {
                case 0:
                    if (this.random.nextBoolean())
                        ptx.create(Foo1.class);
                    else if (this.random.nextBoolean())
                        ptx.create(Foo2.class);
                    else
                        ptx.create(Foo3.class);
                    break;
                case 1:
                    ptx.delete(top1);
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
                this.verifyIndexes(ptx);
            }
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private Top getRandomTop(PermazenTransaction ptx) {
        final NavigableSet<Top> tops = ptx.getAll(Top.class);
        final int num = tops.size();
        if (num == 0 || this.random.nextInt(10) > 7)
            return null;
        return (Top)tops.toArray()[this.random.nextInt(num)];
    }

    private void verifyIndexes(PermazenTransaction ptx) {
        for (Class<?> startType : this.getClasses()) {
            for (Class<?> refType : this.getClasses()) {
                Assert.assertEquals(ptx.queryCompositeIndex(startType, "index1", refType, String.class).asSet(),
                  this.buildIndex1(ptx, refType, startType));
                Assert.assertEquals(ptx.queryCompositeIndex(startType, "index2", Integer.class, refType).asSet(),
                  this.buildIndex2(ptx, refType, startType));
            }
        }
    }

    private <R, T> Set<Tuple3<R, String, T>> buildIndex1(PermazenTransaction ptx, Class<R> ref, Class<T> target) {
        final HashSet<Tuple3<R, String, T>> set = new HashSet<>();
        for (Top top : ptx.getAll(Top.class)) {
            try {
                set.add(new Tuple3<>(ref.cast(top.getRef1()), top.getString(), target.cast(top)));
            } catch (ClassCastException e) {
                continue;
            }
        }
        return set;
    }

    private <R, T> Set<Tuple3<Integer, R, T>> buildIndex2(PermazenTransaction ptx, Class<R> ref, Class<T> target) {
        final HashSet<Tuple3<Integer, R, T>> set = new HashSet<>();
        for (Top top : ptx.getAll(Top.class)) {
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
        final Permazen pdb = BasicTest.newPermazen(IndexedOn2.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NavigableMap<Tuple2<PermazenObject, Thread.State>, NavigableSet<IndexedOn2>> view
              = ptx.queryCompositeIndex(IndexedOn2.class, "index2", PermazenObject.class, Thread.State.class).asMap();

            final IndexedOn2 a = ptx.create(IndexedOn2.class);
            final IndexedOn2 b = ptx.create(IndexedOn2.class);
            final IndexedOn2 c = ptx.create(IndexedOn2.class);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, b, c));

            this.checkValid(false, view);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(null, Thread.State.RUNNABLE)),
              null);

            this.checkValid(true);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, b, c));

            b.setField1(a);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a, c));

            c.setField2(Thread.State.WAITING);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(null, Thread.State.RUNNABLE)),
              buildSet(a));

            this.checkValid(true);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex3() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(IndexedOn3.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NavigableMap<Tuple3<PermazenObject, Thread.State, Integer>, NavigableSet<IndexedOn3>> view
              = ptx.queryCompositeIndex(IndexedOn3.class, "index3",
               PermazenObject.class, Thread.State.class, Integer.class).asMap();

            final IndexedOn3 a = ptx.create(IndexedOn3.class);
            final IndexedOn3 b = ptx.create(IndexedOn3.class);
            final IndexedOn3 c = ptx.create(IndexedOn3.class);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            a.setField3(-1234);
            b.setField3(-1234);
            c.setField3(-1234);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(a, b, c));

            this.checkValid(true);

            a.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(b, c));

            this.checkValid(true);

            b.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(c));

            this.checkValid(false, view);

            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              null);

            this.checkValid(false, view);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);
            c.setField2(Thread.State.TERMINATED);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(a, b, c));

            a.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(b, c));

            this.checkValid(true);

            b.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              buildSet(c));

            this.checkValid(false, view);

            c.setField3(4567);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.TERMINATED, -1234)),
              null);

            this.checkValid(false, view);

            b.setField1(a);
            c.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.RUNNABLE, -1234)),
              null);

            Assert.assertEquals(view.get(new Tuple3<PermazenObject, Thread.State, Integer>(null, Thread.State.RUNNABLE, 4567)),
              buildSet(c));

            this.checkValid(true);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex4() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(IndexedOn4.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NavigableMap<Tuple4<PermazenObject, Thread.State, Integer, String>, NavigableSet<IndexedOn4>> view
              = ptx.queryCompositeIndex(IndexedOn4.class,
               "index4", PermazenObject.class, Thread.State.class, Integer.class, String.class).asMap();

            final IndexedOn4 a = ptx.create(IndexedOn4.class);
            final IndexedOn4 b = ptx.create(IndexedOn4.class);
            final IndexedOn4 c = ptx.create(IndexedOn4.class);

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
              view.get(new Tuple4<PermazenObject, Thread.State, Integer, String>(null, Thread.State.RUNNABLE, -1234, "foobar")),
              buildSet(a, b, c));

            this.checkValid(false, view);

            a.setField1(c);
            b.setField4("janfu");

            Assert.assertEquals(
              view.get(new Tuple4<PermazenObject, Thread.State, Integer, String>(null, Thread.State.RUNNABLE, -1234, "foobar")),
              buildSet(c));

            this.checkValid(true);

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

            this.checkValid(false, view);

            for (Thread.State s : new Thread.State[] {
              Thread.State.TIMED_WAITING, Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.NEW, Thread.State.TERMINATED }) {
                a.setField2(s);
                b.setField2(s);
                c.setField2(s);
                this.checkValid(true);
            }

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex5() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(IndexedOn5.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NavigableMap<Tuple3<Thread.State, Integer, String>, NavigableSet<IndexedOn5>> view
              = ptx.queryCompositeIndex(IndexedOn5.class, "index5", Thread.State.class, Integer.class, String.class).asMap();

            final IndexedOn5 a = ptx.create(IndexedOn5.class);
            final IndexedOn5 b = ptx.create(IndexedOn5.class);
            final IndexedOn5 c = ptx.create(IndexedOn5.class);

            Assert.assertEquals(
              view.get(new Tuple3<Thread.State, Integer, String>(null, 0, null)),
              buildSet(a, b, c));

    // Check: @ValuesList({ @Values("RUNNABLE"), @Values("0"), @Values(nonNulls = true) }),

            checkValid(false);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);
            c.setField2(Thread.State.RUNNABLE);

            checkValid(false);              // all three are duplicates, none are excluded

            a.setField4("not null");
            checkValid(false);              // a excluded, but b and c are still duplicates
            b.setField4("not null");
            checkValid(true);               // a and b excluded, c is unique
            c.setField4("not null");
            checkValid(true);               // all are excluded

            a.setField2(Thread.State.NEW);
            checkValid(true);               // b and c still excluded, a is unique
            b.setField2(Thread.State.NEW);
            checkValid(false);              // only c excluded, a & b are duplicates
            c.setField2(Thread.State.NEW);
            checkValid(false);              // all three are duplicates, none are excluded

            Assert.assertEquals(
              view.get(new Tuple3<Thread.State, Integer, String>(Thread.State.NEW, 0, "not null")),
              buildSet(a, b, c));

    // Check: @ValuesList({ @Values(nonNulls = true), @Values("-1234"), @Values(nonNulls = true) }),

            a.setField3(-1234);
            b.setField3(-1234);
            c.setField3(-1234);
            checkValid(true);               // all are excluded (fields field2 and field4 are both null)

            a.setField2(null);
            checkValid(true);               // b and c still excluded, a is unique
            b.setField4(null);
            checkValid(true);               // only c excluded, but a & b are not duplicates
            a.setField4(null);
            b.setField2(null);
            checkValid(false);              // only c excluded, a & b are now duplicates again
            c.setField4(null);
            c.setField2(null);
            checkValid(false);              // all three are duplicates, none are excluded

            Assert.assertEquals(
              view.get(new Tuple3<Thread.State, Integer, String>(null, -1234, null)),
              buildSet(a, b, c));

    // Check: @ValuesList({ @Values(nulls = true, nonNulls = true), @Values(nulls = true, nonNulls = true), @Values("duplicate") })

            a.setField3(42);
            b.setField3(42);
            c.setField3(42);

            Assert.assertEquals(
              view.get(new Tuple3<Thread.State, Integer, String>(null, 42, null)),
              buildSet(a, b, c));

            a.setField4("duplicate");
            checkValid(false);              // a is excluded, b and c are still duplicates
            b.setField4("duplicate");
            checkValid(true);               // a and b are excluded, c is unique
            c.setField4("duplicate");
            checkValid(true);               // all three are excluded

            Assert.assertEquals(
              view.get(new Tuple3<Thread.State, Integer, String>(null, 42, "duplicate")),
              buildSet(a, b, c));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInvalidCompositeIndex1() throws Exception {
        try {
            BasicTest.newPermazen(InvalidIndexed1.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test
    public void testInvalidCompositeIndex2() throws Exception {
        try {
            BasicTest.newPermazen(InvalidIndexed2.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndexSubTypes() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(A.class, B.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NavigableMap<Tuple2<PermazenObject, Thread.State>, NavigableSet<IndexedOn2>> view
              = ptx.queryCompositeIndex(IndexedOn2.class, "index2", PermazenObject.class, Thread.State.class).asMap();

            final NavigableMap<Tuple2<PermazenObject, Thread.State>, NavigableSet<A>> viewA
              = ptx.queryCompositeIndex(A.class, "index2", PermazenObject.class, Thread.State.class).asMap();

            final NavigableMap<Tuple2<PermazenObject, Thread.State>, NavigableSet<B>> viewB
              = ptx.queryCompositeIndex(B.class, "index2", PermazenObject.class, Thread.State.class).asMap();

            A a = ptx.create(A.class);
            B b = ptx.create(B.class);

            a.setField1(a);
            b.setField1(a);

            a.setField2(Thread.State.RUNNABLE);
            b.setField2(Thread.State.RUNNABLE);

            Assert.assertEquals(view.get(new Tuple2<PermazenObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(a, b));
            Assert.assertEquals(viewA.get(new Tuple2<PermazenObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(a));
            Assert.assertEquals(viewB.get(new Tuple2<PermazenObject, Thread.State>(a, Thread.State.RUNNABLE)), buildSet(b));

            this.checkValid(false, view);

            a.setField1(null);
            b.setField1(null);

            a.setField2(Thread.State.TERMINATED);
            b.setField2(Thread.State.TERMINATED);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeInitialUnique() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(C.class, D.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            C c = ptx.create(C.class);
            D d = ptx.create(D.class);

            this.checkValid(false);

            c.setField1(d);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private void checkValid(boolean valid, Object... extra) {
        final PermazenTransaction ptx = PermazenTransaction.getCurrent();
        try {
            ptx.validate();
            assert valid : "transaction is unexpectedly valid: " + (extra.length > 0 ? extra[0] : null);
        } catch (ValidationException e) {
            assert !valid : "transaction is unexpectedly invalid: " + e;
            this.log.info("got expected {}", e.toString());
        }
    }

// Model Classes

    @PermazenCompositeIndex(storageId = 11, name = "index1", fields = { "ref1", "string" })
    @PermazenCompositeIndex(storageId = 12, name = "index2", fields = { "int", "ref2" })
    @PermazenType(storageId = 99)
    public abstract static class Top implements PermazenObject {

        @PermazenField(storageId = 1, inverseDelete = DeleteAction.NULLIFY)
        public abstract Top getRef1();
        public abstract void setRef1(Top ref1);

        @PermazenField(storageId = 2, inverseDelete = DeleteAction.NULLIFY)
        public abstract Top getRef2();
        public abstract void setRef2(Top ref2);

        @PermazenField(storageId = 3)
        public abstract int getInt();
        public abstract void setInt(int i);

        @PermazenField(storageId = 4)
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

    public interface Fields extends PermazenObject {

        PermazenObject getField1();
        void setField1(PermazenObject x);

        Thread.State getField2();
        void setField2(Thread.State x);

        int getField3();
        void setField3(int x);

        String getField4();
        void setField4(String x);
    }

    @PermazenCompositeIndex(
      name = "index2",
      fields = { "field1", "field2" },
      unique = true,
      uniqueExcludes = @ValuesList({ @Values(nulls = true), @Values("TERMINATED") }))
    @PermazenType
    public interface IndexedOn2 extends Fields {
    }

    @PermazenCompositeIndex(
      name = "index3",
      fields = { "field1", "field2", "field3" },
      unique = true,
      uniqueExcludes = @ValuesList({ @Values(nulls = true), @Values("TERMINATED"), @Values("-1234") }))
    @PermazenType
    public interface IndexedOn3 extends Fields {
    }

    @PermazenCompositeIndex(
      name = "index4",
      fields = { "field1", "field2", "field3", "field4" },
      unique = true,
      uniqueExcludes = {
        @ValuesList({ @Values(nulls = true), @Values("BLOCKED"),          @Values("-1234"), @Values("foobar") }),
        @ValuesList({ @Values(nulls = true), @Values("NEW"),              @Values("-1234"), @Values("foobar") }),
        @ValuesList({ @Values(nulls = true), @Values("TERMINATED"),       @Values("-1234"), @Values("foobar") }),
        @ValuesList({ @Values(nulls = true), @Values("TIMED_WAITING"),    @Values("-1234"), @Values("foobar") }),
        @ValuesList({ @Values(nulls = true), @Values("WAITING"),          @Values("-1234"), @Values("foobar") })
      })
    @PermazenType
    public interface IndexedOn4 extends Fields {
    }

    @PermazenCompositeIndex(
      name = "index5",
      fields = { "field2", "field3", "field4" },
      unique = true,
      uniqueExcludes = {

        // Allow multiple objects with field2=RUNNABLE and field3=0, as long as only one of them has field4=null
        @ValuesList({ @Values("RUNNABLE"), @Values("0"), @Values(nonNulls = true) }),

        // Allow multiple objects with field3=-1234, as long as both field2 and field4 are not null
        @ValuesList({ @Values(nonNulls = true), @Values("-1234"), @Values(nonNulls = true) }),

        // Allow multiple objects with field4="duplicate"
        @ValuesList({ @Values(nulls = true, nonNulls = true), @Values(nulls = true, nonNulls = true), @Values("duplicate") })
      })
    @PermazenType
    public interface IndexedOn5 extends Fields {
    }

    // Invalid - "nulls = true" on primitive field
    @PermazenCompositeIndex(
      name = "index",
      fields = { "field2", "field3", "field4" },
      unique = true,
      uniqueExcludes = @ValuesList({ @Values("RUNNABLE"), @Values(nulls = true), @Values("foobar") }))
    @PermazenType
    public interface InvalidIndexed1 extends Fields {
    }

    // Invalid - no matches
    @PermazenCompositeIndex(
      name = "index",
      fields = { "field2", "field3", "field4" },
      unique = true,
      uniqueExcludes =
        @ValuesList({ @Values(nulls = true, nonNulls = true), @Values(nonNulls = true), @Values(nulls = true, nonNulls = true) }))
    @PermazenType
    public interface InvalidIndexed2 extends Fields {
    }

// Model Classes #3

    @PermazenType
    public abstract static class A implements IndexedOn2 {
    }

    @PermazenType
    public abstract static class B implements IndexedOn2 {
    }

// Model Classes #4

    @PermazenCompositeIndex(name = "foo", fields = { "field1", "field2", "field3", "field4" }, unique = true)
    public interface Unique {
    }

    @PermazenType
    public abstract static class C implements Fields, Unique {
    }

    @PermazenType
    public abstract static class D implements Fields, Unique {
    }
}
