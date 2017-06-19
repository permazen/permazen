
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.NavigableSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.index.Index;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.NavigableSets;
import org.testng.annotations.Test;

public class IndexQueryTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testSharedStorageId() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Account.class, Foo.class, Bar.class, Jam.class);

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final Account a1 = jtx.get(new ObjId("0A1111111111A001"), Account.class);
            final Account a2 = jtx.get(new ObjId("0A2222222222A002"), Account.class);
            jtx.recreate(a1);
            jtx.recreate(a2);

            final Foo f1 = jtx.get(new ObjId("141111111111F001"), Foo.class);
            final Foo f2 = jtx.get(new ObjId("142222222222F002"), Foo.class);
            jtx.recreate(f1);
            jtx.recreate(f2);

            final Bar b1 = jtx.get(new ObjId("281111111111BA01"), Bar.class);
            final Bar b2 = jtx.get(new ObjId("282222222222BA02"), Bar.class);
            jtx.recreate(b1);
            jtx.recreate(b2);

            Jam j1 = jtx.create(Jam.class);
            j1.setAccount(a1);

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
                jtx.queryIndex(HasAccount.class, "name", Account.class);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }

            TestSupport.checkMap(jtx.queryIndex(Jam.class, "account", Account.class).asMap(),
              buildMap(a1, buildSet(j1)));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryIndexType() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(HasNameImpl.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final HasNameImpl h1 = jtx.create(HasNameImpl.class);
            final HasNameImpl h2 = jtx.create(HasNameImpl.class);

            h1.setName("h1");
            h2.setName("h2");

            TestSupport.checkMap(jtx.queryIndex(HasNameImpl.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(jtx.queryIndex(HasName.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(jtx.queryIndex(JObject.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(jtx.queryIndex(Object.class, "name", String.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            TestSupport.checkMap(jtx.queryIndex(HasNameImpl.class, "name", Object.class).asMap(),
              buildMap("h1", buildSet(h1), "h2", buildSet(h2)));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public interface FooBar {
    }

    public interface HasAccount extends JObject {

        @JField(storageId = 1)
        Account getAccount();
        void setAccount(Account x);
    }

    @JSimpleClass(storageId = 10)
    public abstract static class Account implements JObject {

        @JField(storageId = 2)
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

        public static Index<Account, HasAccount> queryHasAccount() {
            return JTransaction.getCurrent().queryIndex(HasAccount.class, "account", Account.class);
        }

        public static Index<Account, Foo> queryFoo() {
            return JTransaction.getCurrent().queryIndex(Foo.class, "account", Account.class);
        }

        public static Index<Account, Bar> queryBar() {
            return JTransaction.getCurrent().queryIndex(Bar.class, "account", Account.class);
        }

        public static Index<Account, FooBar> queryFooBar() {
            return JTransaction.getCurrent().queryIndex(FooBar.class, "account", Account.class);
        }
    }

    @JSimpleClass(storageId = 20)
    public abstract static class Foo implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Foo@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 30)
    public abstract static class Jam implements HasAccount {

        @Override
        public String toString() {
            return "Jam@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 40)
    public abstract static class Bar implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Bar@" + this.getObjId();
        }
    }

    public interface HasName {

        @JField(indexed = true)
        String getName();
        void setName(String name);
    }

    @JSimpleClass
    public abstract static class HasNameImpl implements JObject, HasName {

        @Override
        public abstract String getName();
        @Override
        public abstract void setName(String name);
    }
}

