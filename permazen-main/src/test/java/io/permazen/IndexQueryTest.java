
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;
import io.permazen.index.Index1;
import io.permazen.test.TestSupport;
import io.permazen.util.BoundType;
import io.permazen.util.Bounds;
import io.permazen.util.NavigableSets;

import java.util.NavigableSet;

import org.testng.annotations.Test;

public class IndexQueryTest extends MainTestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testSharedStorageId() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Account.class, Foo.class, Bar.class, Jam.class);

        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Account a1 = ptx.get(new ObjId("0A1111111111A001"), Account.class);
            final Account a2 = ptx.get(new ObjId("0A2222222222A002"), Account.class);
            ptx.recreate(a1);
            ptx.recreate(a2);

            final Foo f1 = ptx.get(new ObjId("141111111111F001"), Foo.class);
            final Foo f2 = ptx.get(new ObjId("142222222222F002"), Foo.class);
            ptx.recreate(f1);
            ptx.recreate(f2);

            final Bar b1 = ptx.get(new ObjId("281111111111BA01"), Bar.class);
            final Bar b2 = ptx.get(new ObjId("282222222222BA02"), Bar.class);
            ptx.recreate(b1);
            ptx.recreate(b2);

            Jam j1 = ptx.create(Jam.class);
            j1.setAccount(a1);
            j1.setAge(123);

            f1.setAccount(a1);
            f2.setAccount(a2);
            b1.setAccount(a1);
            b2.setAccount(a2);

        // Index queries

            TestSupport.checkSet(a1.getFoos(), buildSet(f1));
            TestSupport.checkSet(a1.getBars(), buildSet(b1));
            TestSupport.checkSet(a1.getHasAccounts(), buildSet(f1, b1, j1));

            TestSupport.checkSet(a2.getFoos(), buildSet(f2));
            TestSupport.checkSet(a2.getBars(), buildSet(b2));
            TestSupport.checkSet(a2.getHasAccounts(), buildSet(f2, b2));

            TestSupport.checkSet(a1.getFooBars(), buildSet(f1, b1));
            TestSupport.checkSet(a2.getFooBars(), buildSet(f2, b2));

            try {
                ptx.querySimpleIndex(HasAccount.class, "name", Account.class);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }

            TestSupport.checkMap(ptx.querySimpleIndex(Jam.class, "account", Account.class).asMap(),
              buildMap(a1, buildSet(j1)));

            TestSupport.checkMap(ptx.querySimpleIndex(Jam.class, "age", Integer.class).asMap(),
              buildMap(123, buildSet(j1)));

            TestSupport.checkMap(ptx.querySimpleIndex(Jam.class, "age", int.class).asMap(),
              buildMap(123, buildSet(j1)));

        // PermazenObject.getReferring()

            TestSupport.checkSet(a1.findReferring(Foo.class, "account"), buildSet(f1));
            TestSupport.checkSet(a1.findReferring(Bar.class, "account"), buildSet(b1));
            TestSupport.checkSet(a1.findReferring(Jam.class, "account"), buildSet(j1));
            TestSupport.checkSet(a1.findReferring(FooBar.class, "account"), buildSet(f1, b1));
            TestSupport.checkSet(a1.findReferring(Object.class, "account"), buildSet(f1, b1, j1));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryIndexType() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(HasNameImpl.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final HasNameImpl h1 = ptx.create(HasNameImpl.class);
            final HasNameImpl h2 = ptx.create(HasNameImpl.class);

            h1.setName("h1");
            h2.setName("h2");

            TestSupport.checkMap(ptx.querySimpleIndex(HasNameImpl.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(ptx.querySimpleIndex(HasName.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(ptx.querySimpleIndex(PermazenObject.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(ptx.querySimpleIndex(Object.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(ptx.querySimpleIndex(HasNameImpl.class, "name", Object.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBoundedIndexes() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(HasNameAndAge.class);
        final PermazenTransaction ptx = pdb.createTransaction();
        PermazenTransaction.setCurrent(ptx);
        try {

            final HasNameAndAge alice = ptx.get(new ObjId("07aaaaaaaaaaaaaa"), HasNameAndAge.class);
            final HasNameAndAge bob = ptx.get(new ObjId("07bbbbbbbbbbbbbb"), HasNameAndAge.class);
            final HasNameAndAge bobby = ptx.get(new ObjId("07bbbbbbbbbbbbbc"), HasNameAndAge.class);
            final HasNameAndAge eve = ptx.get(new ObjId("07eeeeeeeeeeeeee"), HasNameAndAge.class);

            alice.recreate();
            alice.setName("alice");
            alice.setAge(12);

            bob.recreate();
            bob.setName("bob");
            bob.setAge(42);

            bobby.recreate();
            bobby.setName("bob");
            bobby.setAge(8);

            eve.recreate();
            eve.setName("eve");
            eve.setAge(73);

            final Index1<String, HasNameAndAge> nameIndex = ptx.querySimpleIndex(HasNameAndAge.class, "name", String.class);
            final Index1<Integer, HasNameAndAge> ageIndex = ptx.querySimpleIndex(HasNameAndAge.class, "age", Integer.class);

        // Restrict name

            TestSupport.checkMap(nameIndex.asMap(), buildMap(
              "alice", buildSet(alice),
              "bob", buildSet(bob, bobby),
              "eve", buildSet(eve)));

            TestSupport.checkMap(nameIndex.withValueBounds(new Bounds<>()).asMap(), buildMap(
              "alice", buildSet(alice),
              "bob", buildSet(bob, bobby),
              "eve", buildSet(eve)));

            TestSupport.checkMap(nameIndex.withValueBounds(new Bounds<>("darla", "doris")).asMap(), buildMap());

            TestSupport.checkMap(nameIndex.withValueBounds(new Bounds<>("bob", "boc")).asMap(), buildMap(
              "bob", buildSet(bob, bobby)));

            TestSupport.checkMap(
              nameIndex.withValueBounds(new Bounds<>("bob", false, "boba", false)).asMap(), buildMap());

            TestSupport.checkMap(
              nameIndex.withValueBounds(new Bounds<>("bob", true, "boba", false)).asMap(), buildMap(
              "bob", buildSet(bob, bobby)));

        // Restrict age

            TestSupport.checkMap(ageIndex.asMap(), buildMap(
              12, buildSet(alice),
              42, buildSet(bob),
              8,  buildSet(bobby),
              73, buildSet(eve)));

            TestSupport.checkMap(ageIndex.withValueBounds(new Bounds<>(10, BoundType.INCLUSIVE, false)).asMap(), buildMap(
              12, buildSet(alice),
              42, buildSet(bob),
              73, buildSet(eve)));

            TestSupport.checkMap(ageIndex.withValueBounds(new Bounds<>(10, 50)).asMap(), buildMap(
              12, buildSet(alice),
              42, buildSet(bob)));

            TestSupport.checkMap(ageIndex.withValueBounds(new Bounds<>(12, 42)).asMap(), buildMap(
              12, buildSet(alice)));

            TestSupport.checkMap(
              ageIndex.withValueBounds(new Bounds<>(12, false, 42, true)).asMap(), buildMap(
              42, buildSet(bob)));

        // Restrict target

            TestSupport.checkMap(nameIndex.withTargetBounds(new Bounds<>(bob, bobby)).asMap(), buildMap(
              "bob", buildSet(bob)));

            TestSupport.checkMap(nameIndex.withTargetBounds(new Bounds<>(bob, true, bobby, true)).asMap(), buildMap(
              "bob", buildSet(bob, bobby)));

            TestSupport.checkMap(nameIndex.withTargetBounds(new Bounds<>(bob, false, bobby, true)).asMap(), buildMap(
              "bob", buildSet(bobby)));

            TestSupport.checkMap(nameIndex.withTargetBounds(new Bounds<>(bob, false, bobby, false)).asMap(), buildMap());

            TestSupport.checkMap(nameIndex.withTargetBounds(new Bounds<>(alice, eve)).asMap(), buildMap(
              "alice", buildSet(alice),
              "bob", buildSet(bob, bobby)));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    public interface FooBar {
    }

    public interface HasAccount extends PermazenObject {

        @PermazenField(storageId = 1)
        Account getAccount();
        void setAccount(Account x);
    }

    @PermazenType(storageId = 10)
    public abstract static class Account implements PermazenObject {

        @PermazenField(storageId = 2)
        public abstract String getName();
        public abstract void setName(String x);

        public NavigableSet<Foo> getFoos() {
            final NavigableSet<Foo> foos = Account.queryFoo().asMap().get(this);
            return foos != null ? foos : NavigableSets.<Foo>empty();
        }

        public NavigableSet<Bar> getBars() {
            final NavigableSet<Bar> bars = Account.queryBar().asMap().get(this);
            return bars != null ? bars : NavigableSets.<Bar>empty();
        }

        public NavigableSet<HasAccount> getHasAccounts() {
            final NavigableSet<HasAccount> hasAccounts = Account.queryHasAccount().asMap().get(this);
            return hasAccounts != null ? hasAccounts : NavigableSets.<HasAccount>empty();
        }

        public NavigableSet<FooBar> getFooBars() {
            final NavigableSet<FooBar> fooBars = Account.queryFooBar().asMap().get(this);
            return fooBars != null ? fooBars : NavigableSets.<FooBar>empty();
        }

        public static Index1<Account, HasAccount> queryHasAccount() {
            return PermazenTransaction.getCurrent().querySimpleIndex(HasAccount.class, "account", Account.class);
        }

        public static Index1<Account, Foo> queryFoo() {
            return PermazenTransaction.getCurrent().querySimpleIndex(Foo.class, "account", Account.class);
        }

        public static Index1<Account, Bar> queryBar() {
            return PermazenTransaction.getCurrent().querySimpleIndex(Bar.class, "account", Account.class);
        }

        public static Index1<Account, FooBar> queryFooBar() {
            return PermazenTransaction.getCurrent().querySimpleIndex(FooBar.class, "account", Account.class);
        }

        public <R> NavigableSet<R> findReferring(Class<R> type, String fieldName) {
            final NavigableSet<R> set = this.getPermazenTransaction()
              .querySimpleIndex(type, fieldName, Object.class)
              .asMap()
              .get(this);
            return set != null ? set : NavigableSets.empty();
        }
    }

    @PermazenType(storageId = 20)
    public abstract static class Foo implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Foo@" + this.getObjId();
        }
    }

    public interface HasAge {

        @PermazenField(indexed = true)
        int getAge();
        void setAge(int age);
    }

    @PermazenType(storageId = 30)
    public abstract static class Jam implements HasAccount, HasAge {

        @Override
        public String toString() {
            return "Jam@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 40)
    public abstract static class Bar implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Bar@" + this.getObjId();
        }
    }

    public interface HasName {

        @PermazenField(indexed = true)
        String getName();
        void setName(String name);
    }

    @PermazenType
    public abstract static class HasNameImpl implements PermazenObject, HasName {

        @Override
        public abstract String getName();
        @Override
        public abstract void setName(String name);
    }

    @PermazenType(storageId = 7)
    public abstract static class HasNameAndAge implements PermazenObject, HasName, HasAge {
    }
}
