
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnChange;
import org.jsimpledb.change.SimpleFieldChange;
import org.testng.annotations.Test;

public class GenericsFunTest extends TestSupport {

    @Test
    public void testGenerics1() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Widget.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            jtx.querySimpleField(AbstractData.class, "name", String.class);
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics2() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(AccountEvent.class, Account.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            jtx.querySimpleField(AbstractData.class, "name", String.class);
            jtx.querySimpleField(Account.class, "name", String.class);
            jtx.querySimpleField(AccountEvent.class, "account", Account.class);
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public abstract static class AbstractData<T extends AbstractData<T>> implements JObject {

        @JField(storageId = 201, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange("name")
        public void nameChanged(SimpleFieldChange<? extends AbstractData<?>, String> change) {
        }
    }

    public abstract static class AbstractAccountData<T extends AbstractAccountData<T>> extends AbstractData<T> {

        @JField(storageId = 101)
        public abstract Account getAccount();
        public abstract void setAccount(Account name);
    }

    @JSimpleClass(storageId = 200)
    public abstract static class Account extends AbstractData<Account> {
    }

    @JSimpleClass(storageId = 300)
    public abstract static class AccountEvent extends AbstractAccountData<AccountEvent> {

        @JField(storageId = 301)
        public abstract int getEventId();
        public abstract void setEventId(int eventId);
    }

    @JSimpleClass(storageId = 400)
    public abstract static class Widget extends AbstractData<Widget> {
    }
}

