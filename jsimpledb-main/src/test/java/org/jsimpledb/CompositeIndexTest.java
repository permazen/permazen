
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.annotation.JCompositeIndex;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.tuple.Tuple3;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompositeIndexTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testCompositeIndex() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final JSimpleDB jdb = new JSimpleDB(db, 2, null, this.getClasses());

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
                set.add(new Tuple3<R, String, T>(ref.cast(top.getRef1()), top.getString(), target.cast(top)));
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
                set.add(new Tuple3<Integer, R, T>(top.getInt(), ref.cast(top.getRef2()), target.cast(top)));
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

// Model Classes

    @JSimpleClass(storageId = 99, compositeIndexes = {
      @JCompositeIndex(storageId = 11, name = "index1", fields = { "ref1", "string" }),
      @JCompositeIndex(storageId = 12, name = "index2", fields = { "int", "ref2" }),
    })
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

    @JSimpleClass(storageId = 10)
    public abstract static class Foo1 extends Top {
    }

    @JSimpleClass(storageId = 20)
    public abstract static class Foo2 extends Top {
    }

    @JSimpleClass(storageId = 30)
    public abstract static class Foo3 extends Top {
    }
}

