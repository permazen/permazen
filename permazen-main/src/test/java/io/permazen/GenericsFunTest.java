
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SimpleFieldChange;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class GenericsFunTest extends MainTestSupport {

    @Test
    public void testGenerics1() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Widget.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            ptx.querySimpleIndex(AbstractData.class, "name", String.class);
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics2() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(AccountEvent.class, Account.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            ptx.querySimpleIndex(AbstractData.class, "name", String.class);
            ptx.querySimpleIndex(Account.class, "name", String.class);
            ptx.querySimpleIndex(AccountEvent.class, "account", Account.class);
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics3() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Class2.class, Class3.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            final Class2 c2 = ptx.create(Class2.class);
            final Class3 c3 = ptx.create(Class3.class);
            c2.setFriend(c3);
            c3.setFriend(c2);
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics4() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Class2.class, Class3.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            final Class2 c2 = ptx.create(Class2.class);
            try {
                c2.setWrong(c2);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics5() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(ListSub1.class, ListSub2.class);

        final PermazenClass<ListSub1> jclass1 = pdb.getPermazenClass(ListSub1.class);
        final PermazenClass<ListSub2> jclass2 = pdb.getPermazenClass(ListSub2.class);

        final PermazenSimpleField field1 = Util.findSimpleField(jclass1, "list.element");
        final PermazenSimpleField field2 = Util.findSimpleField(jclass2, "list.element");

        Assert.assertEquals(field1.getTypeToken(), TypeToken.of(ListSub2.class));
        Assert.assertEquals(field2.getTypeToken(), TypeToken.of(ListSub1.class));

        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            final ListSub1 sub1 = ptx.create(ListSub1.class);
            try {
                sub1.addListWrong(sub1);            // ListSub1's list only accepts object of type ListSub2
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testGenerics6() throws Exception {
        BasicTest.newPermazen(GenericBeanProperty.class);
    }

    @Test
    public void testGenerics7() throws Exception {
        BasicTest.newPermazen(GenericBeanProperty2.class);
    }

    @Test
    public void testGenerics8() throws Exception {
        BasicTest.newPermazen(Model8B.class, HappyPerson.class);
    }

// Model Classes #1

    public abstract static class AbstractData<T extends AbstractData<T>> implements PermazenObject {

        @PermazenField(storageId = 201, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange("name")
        public void nameChanged(SimpleFieldChange<? extends AbstractData<?>, String> change) {
        }
    }

    public abstract static class AbstractAccountData<T extends AbstractAccountData<T>> extends AbstractData<T> {

        @PermazenField(storageId = 101)
        public abstract Account getAccount();
        public abstract void setAccount(Account name);
    }

    @PermazenType(storageId = 200)
    public abstract static class Account extends AbstractData<Account> {
    }

    @PermazenType(storageId = 300)
    public abstract static class AccountEvent extends AbstractAccountData<AccountEvent> {

        @PermazenField(storageId = 301)
        public abstract int getEventId();
        public abstract void setEventId(int eventId);
    }

    @PermazenType(storageId = 400)
    public abstract static class Widget extends AbstractData<Widget> {
    }

// Model Classes #2

    public abstract static class Class1<T> implements PermazenObject {

        public abstract T getFriend();
        public abstract void setFriend(T friend);

        @SuppressWarnings("unchecked")
        public void setWrong(Object obj) {
            this.setFriend((T)obj);
        }
    }

    @PermazenType
    public abstract static class Class2 extends Class1<Class3> {
    }

    @PermazenType
    public abstract static class Class3 extends Class1<Class2> {
    }

// Model Classes #3

    public abstract static class ListSuper<E extends PermazenObject> implements PermazenObject {

        public abstract List<E> getList();

        @SuppressWarnings("unchecked")
        public void addListWrong(Object obj) {
            this.getList().add((E)obj);
        }
    }

    @PermazenType
    public abstract static class ListSub1 extends ListSuper<ListSub2> {
    }

    @PermazenType
    public abstract static class ListSub2 extends ListSuper<ListSub1> {
    }

// Model Classes #4

    public interface GenericBeanPropertySupport<T> {

        T getProperty();
        void setProperty(T value);
    }

    @PermazenType
    public abstract static class GenericBeanProperty implements GenericBeanPropertySupport<String> {

        @Override
        @PermazenField(indexed = true)
        public abstract String getProperty();
    }

    @PermazenType
    public abstract static class GenericBeanProperty2 implements GenericBeanPropertySupport<String> {

        @Override
        @PermazenField(indexed = true)
        public abstract String getProperty();
        @Override
        public abstract void setProperty(String x);
    }

// Model Classes #8

    public interface Person {
        String getName();
        void setName(String x);
    }

    @PermazenType
    public interface HappyPerson extends Person {
    }

    public abstract static class Model8A<P extends Person> {

        @PermazenField
        public abstract P getPerson();
        public abstract void setPerson(P person);
    }

    @PermazenType
    public abstract static class Model8B extends Model8A<HappyPerson> {

        @Override
        public abstract HappyPerson getPerson();
        @Override
        public abstract void setPerson(HappyPerson person);
    }
}
