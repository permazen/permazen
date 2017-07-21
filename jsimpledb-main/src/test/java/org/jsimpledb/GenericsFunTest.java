
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.Collections;
import java.util.List;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnChange;
import org.jsimpledb.change.FieldChange;
import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GenericsFunTest extends TestSupport {

    @Test
    public void testGenerics1() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Widget.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            jtx.queryIndex(AbstractData.class, "name", String.class);
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
            jtx.queryIndex(AbstractData.class, "name", String.class);
            jtx.queryIndex(Account.class, "name", String.class);
            jtx.queryIndex(AccountEvent.class, "account", Account.class);
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics3() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Class2.class, Class3.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final Class2 c2 = jtx.create(Class2.class);
            final Class3 c3 = jtx.create(Class3.class);
            c2.setFriend(c3);
            c3.setFriend(c2);
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics4() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Class2.class, Class3.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final Class2 c2 = jtx.create(Class2.class);
            try {
                c2.setWrong(c2);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics5() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(ListSub1.class, ListSub2.class);

        final ReferencePath path1 = jdb.parseReferencePath(ListSub1.class, "list.element");
        final ReferencePath path2 = jdb.parseReferencePath(ListSub2.class, "list.element");

        Assert.assertEquals(path1.getTargetFieldTypes(), Collections.singleton(TypeToken.of(ListSub2.class)));
        Assert.assertEquals(path2.getTargetFieldTypes(), Collections.singleton(TypeToken.of(ListSub1.class)));

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final ListSub1 sub1 = jtx.create(ListSub1.class);
            try {
                sub1.addListWrong(sub1);            // ListSub1's list only accepts object of type ListSub2
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics6() throws Exception {
        BasicTest.getJSimpleDB(GenericBeanProperty.class);
    }

// Model Classes #1

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

// Model Classes #2

    public abstract static class Class1<T> implements JObject {

        public abstract T getFriend();
        public abstract void setFriend(T friend);

        @SuppressWarnings("unchecked")
        public void setWrong(Object obj) {
            this.setFriend((T)obj);
        }
    }

    @JSimpleClass
    public abstract static class Class2 extends Class1<Class3> {
    }

    @JSimpleClass
    public abstract static class Class3 extends Class1<Class2> {
    }

// Model Classes #3

    public abstract static class ListSuper<E extends JObject> implements JObject {

        public abstract List<E> getList();

        @SuppressWarnings("unchecked")
        public void addListWrong(Object obj) {
            this.getList().add((E)obj);
        }
    }

    @JSimpleClass
    public abstract static class ListSub1 extends ListSuper<ListSub2> {
    }

    @JSimpleClass
    public abstract static class ListSub2 extends ListSuper<ListSub1> {
    }

// Model Classes #4

    public interface GenericBeanPropertySupport<T> {

        T getProperty();
        void setProperty(T value);
    }

    @JSimpleClass
    public abstract static class GenericBeanProperty implements GenericBeanPropertySupport<String> {

        @Override
        @JField(indexed = true)
        public abstract String getProperty();
    }
}

