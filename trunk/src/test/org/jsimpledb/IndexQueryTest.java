
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.annotation.IndexQuery;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.util.NavigableSets;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IndexQueryTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testSharedStorageId() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Account.class, Foo.class, Bar.class, Jam.class);

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            Account a1 = jtx.create(Account.class);
            Account a2 = jtx.create(Account.class);

            Foo f1 = jtx.create(Foo.class);
            Foo f2 = jtx.create(Foo.class);

            Bar b1 = jtx.create(Bar.class);
            Bar b2 = jtx.create(Bar.class);

            Jam j1 = jtx.create(Jam.class);
            j1.setAccount(a1);

            f1.setAccount(a1);
            f2.setAccount(a2);
            b1.setAccount(a1);
            b2.setAccount(a2);

            Assert.assertEquals(a1.getFoos(), buildSet(f1));
            Assert.assertEquals(a1.getBars(), buildSet(b1));
            Assert.assertEquals(a1.getHasAccounts(), buildSet(f1, b1, j1));

            Assert.assertEquals(a2.getFoos(), buildSet(f2));
            Assert.assertEquals(a2.getBars(), buildSet(b2));
            Assert.assertEquals(a2.getHasAccounts(), buildSet(f2, b2));

            Assert.assertEquals(a1.getFooBars(), buildSet(f1, b1));
            Assert.assertEquals(a2.getFooBars(), buildSet(f2, b2));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public interface FooBar {
    }

    public interface HasAccount extends JObject {

        @JField(storageId = 110)
        Account getAccount();
        void setAccount(Account x);
    }

    @JSimpleClass(storageId = 120)
    public abstract static class Account implements JObject {

        @JField(storageId = 121)
        public abstract String getName();
        public abstract void setName(String x);

        public NavigableSet<Foo> getFoos() {
            final NavigableSet<Foo> foos = this.queryFoo().get(this);
            return foos != null ? foos : NavigableSets.<Foo>empty();
        }

        public NavigableSet<Bar> getBars() {
            final NavigableSet<Bar> bars = this.queryBar().get(this);
            return bars != null ? bars : NavigableSets.<Bar>empty();
        }

        public NavigableSet<HasAccount> getHasAccounts() {
            final NavigableSet<HasAccount> hasAccounts = this.queryHasAccount().get(this);
            return hasAccounts != null ? hasAccounts : NavigableSets.<HasAccount>empty();
        }

        public NavigableSet<FooBar> getFooBars() {
            final NavigableSet<FooBar> fooBars = this.queryFooBar().get(this);
            return fooBars != null ? fooBars : NavigableSets.<FooBar>empty();
        }

        @IndexQuery(startType = HasAccount.class, value = "account")
        protected abstract NavigableMap<Account, NavigableSet<HasAccount>> queryHasAccount();

        @IndexQuery(startType = Foo.class, value = "account")
        protected abstract NavigableMap<Account, NavigableSet<Foo>> queryFoo();

        @IndexQuery(startType = Bar.class, value = "account")
        protected abstract NavigableMap<Account, NavigableSet<Bar>> queryBar();

        @IndexQuery(startType = FooBar.class, value = "account")
        protected abstract NavigableMap<Account, NavigableSet<FooBar>> queryFooBar();
    }

    @JSimpleClass(storageId = 130)
    public abstract static class Foo implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Foo@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 135)
    public abstract static class Jam implements HasAccount {

        @Override
        public String toString() {
            return "Jam@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 140)
    public abstract static class Bar implements HasAccount, FooBar {

        @Override
        public String toString() {
            return "Bar@" + this.getObjId();
        }
    }
}

