
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.FieldChange;
import io.permazen.change.SetFieldAdd;
import io.permazen.change.SetFieldChange;
import io.permazen.change.SimpleFieldChange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OnChangeMultipleTest extends MainTestSupport {

    private static final ThreadLocal<ArrayList<FieldChange<?>>> EVENTS = ThreadLocal.withInitial(ArrayList::new);

    @Test
    public void testMultiFieldChange() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Person.class, Person2.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            final Person p1 = tx.create(Person.class);
            final Person2 p2 = tx.create(Person2.class);

            this.verify();

            p1.setName("Person #1");
            this.verify(new SimpleFieldChange<>(p1, "name", null, "Person #1"));

            p1.getFriends().add(p2);
            this.verify(new SetFieldAdd<Person, Person>(p1, "friends", p2));

            p2.setName("Person #2");
            this.verify(new SimpleFieldChange<Person, String>(p2, "name", null, "Person #2"));

            final Person2 p3 = tx.create(Person2.class);
            p2.getFriends().add(p3);
            this.verify(new SetFieldAdd<Person, Person>(p2, "friends", p3));

            p3.setName("Person #3");
            this.verify(
              new SimpleFieldChange<Person, String>(p3, "name", null, "Person #3"),     // one for p2 (friends.element.name)
              new SimpleFieldChange<Person, String>(p3, "name", null, "Person #3"));    // one for p3 (name)

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test(dataProvider = "invalidClasses")
    public void testInvalidOnChange(List<Class<?>> classes) throws Exception {
        try {
            BasicTest.newPermazen(classes).initialize();
            assert false;
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test(dataProvider = "validClasses")
    public void testValidOnChange(List<Class<?>> classes) throws Exception {
        try {
            BasicTest.newPermazen(classes);
        } catch (Exception e) {
            assert false;
        }
    }

    @DataProvider(name = "validClasses")
    public Object[][] valid() {
        return new Object[][] {
            { buildList(ValidClass1.class) },
            { buildList(ValidClass2.class) },
        };
    }

    @DataProvider(name = "invalidClasses")
    public Object[][] invalid() {
        return new Object[][] {
            { buildList(InvalidClass1.class) },
            { buildList(InvalidClass2.class) },
            { buildList(InvalidClass3.class) },
            { buildList(InvalidClass4.class) },
        };
    }

    private void verify(FieldChange<?>... changes) {
        Assert.assertEquals(EVENTS.get(), Arrays.asList(changes));
        EVENTS.get().clear();
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenSetField(storageId = 103, element = @PermazenField(storageId = 104))
        public abstract Set<Person> getFriends();

        @OnChange
        private void anyFieldChange(FieldChange<?> change) {
            EVENTS.get().add(change);
        }
    }

    @PermazenType(storageId = 200)
    public abstract static class Person2 extends Person {

        @OnChange(path = "->friends", value = "name")
        private void friendNameChange(FieldChange<?> change) {
            EVENTS.get().add(change);
        }
    }

// Valid/Invalid Classes

    @PermazenType(storageId = 100)
    public abstract static class InvalidClass1 implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @OnChange
        private void anyFieldChange(SimpleFieldChange<InvalidClass1, String> change) {
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class InvalidClass2 implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract Counter getCounter();

        @OnChange
        private void anyFieldChange(FieldChange<InvalidClass2> change) {
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class InvalidClass3 implements PermazenObject {

        @OnChange
        private void anyFieldChange(FieldChange<InvalidClass3> change) {
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class InvalidClass4 implements PermazenObject {

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @OnChange
        private void anyFieldChange(SetFieldChange<InvalidClass4> change) {
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class ValidClass1 implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @OnChange
        private void anyFieldChange(SimpleFieldChange<ValidClass1, ?> change) {
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class ValidClass2 implements PermazenObject {

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @OnChange
        private void anyFieldChange(Object change) {
        }
    }
}
