
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.FieldChange;
import io.permazen.change.ListFieldChange;
import io.permazen.change.SetFieldChange;
import io.permazen.change.SimpleFieldChange;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OnChangeWildcardTest extends MainTestSupport {

    private static final String NAME_ID = "name";
    private static final String AGE_ID = "age";

    @Test
    public void testWildcardChanges() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

        // Setup

            final Person p1 = tx.create(Person.class);
            final Person p2 = tx.create(Person.class);
            final Person p3 = tx.create(Person.class);
            final Person p4 = tx.create(Person.class);
            final Person p5 = tx.create(Person.class);

        /*
            p1 -> p2
            p2 -> p3, p4
            p3 -> p1, p4, p5
        */
            p1.getFriends().add(p2);
            p2.getFriends().add(p3);
            p2.getFriends().add(p4);
            p3.getFriends().add(p1);
            p3.getFriends().add(p4);
            p3.getFriends().add(p5);

            this.reset(p1, p2, p3, p4, p5);
            p1.setName("person1");
            p1.setAge(11);
            p1.check(
                buildSet(pair(p1, NAME_ID), pair(p1, AGE_ID)),
                buildSet(pair(p1, NAME_ID), pair(p1, AGE_ID)),
                null,
                null,
                buildSet(pair(p1, AGE_ID)));
            p2.check(
                null,
                null,
                null,
                buildSet(pair(p1, NAME_ID), pair(p1, AGE_ID)),
                null);
            p3.check(
                null,
                null,
                buildSet(pair(p1, NAME_ID), pair(p1, AGE_ID)),
                null,
                null);
            p4.check(null, null, null, null, null);
            p5.check(null, null, null, null, null);

            this.reset(p1, p2, p3, p4, p5);
            p2.setName("person2");
            p3.setAge(33);
            p1.check(
                null,
                null,
                buildSet(pair(p2, NAME_ID)),
                buildSet(pair(p3, AGE_ID)),
                null);
            p2.check(
                buildSet(pair(p2, NAME_ID)),
                buildSet(pair(p2, NAME_ID)),
                buildSet(pair(p3, AGE_ID)),
                null,
                null);
            p3.check(
                buildSet(pair(p3, AGE_ID)),
                buildSet(pair(p3, AGE_ID)),
                null,
                buildSet(pair(p2, NAME_ID)),
                buildSet(pair(p3, AGE_ID)));
            p4.check(null, null, null, null, null);
            p5.check(null, null, null, null, null);

            this.reset(p1, p2, p3, p4, p5);
            p5.setAge(55);
            p1.check(null, null, null, null, null);
            p2.check(
                null,
                null,
                null,
                buildSet(pair(p5, AGE_ID)),
                null);
            p3.check(
                null,
                null,
                buildSet(pair(p5, AGE_ID)),
                null,
                null);
            p4.check(null, null, null, null, null);
            p5.check(
                buildSet(pair(p5, AGE_ID)),
                buildSet(pair(p5, AGE_ID)),
                null,
                null,
                buildSet(pair(p5, AGE_ID)));

            tx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test(dataProvider = "bogusPaths")
    public void testBogusPaths(Class<? extends PermazenObject> cl) throws Exception {
        try {
            BasicTest.newPermazen(Person.class, cl).initialize();
            assert false;
        } catch (IllegalArgumentException e) {
            this.log.info("got expected exception from {}: {}", cl, e.toString());
        }
    }

    @DataProvider(name = "bogusPaths")
    public Object[][] genBogusPaths() {
        return new Object[][] {
            { Bogus1.class },
            { Bogus2.class },
            { Bogus3.class },
            { Bogus4.class },
        };
    }

    private void reset(Person... people) {
        for (Person person : people)
            person.reset();
    }

    private static Pair pair(Person p, String fieldName) {
        return new Pair(p, fieldName);
    }

    public static class Pair extends Tuple2<PermazenObject, String> {

        public Pair(FieldChange<?> change) {
            this(change.getJObject(), change.getFieldName());
        }

        public Pair(PermazenObject pobj, String fieldName) {
            super(pobj, fieldName);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        private final HashSet<Pair> changes1 = new HashSet<>();
        private final HashSet<Pair> changes2 = new HashSet<>();
        private final HashSet<Pair> changes3 = new HashSet<>();
        private final HashSet<Pair> changes4 = new HashSet<>();
        private final HashSet<Pair> changes5 = new HashSet<>();

        public HashSet<Pair> getChanges1() {
            return this.changes1;
        }

        public HashSet<Pair> getChanges2() {
            return this.changes2;
        }

        public HashSet<Pair> getChanges3() {
            return this.changes3;
        }

        public HashSet<Pair> getChanges4() {
            return this.changes4;
        }

        public HashSet<Pair> getChanges5() {
            return this.changes5;
        }

        public void reset() {
            this.changes1.clear();
            this.changes2.clear();
            this.changes3.clear();
            this.changes4.clear();
            this.changes5.clear();
        }

        public void check(Set<?> set1, Set<?> set2, Set<?> set3, Set<?> set4, Set<?> set5) {
            TestSupport.checkSet(this.changes1, set1 != null ? set1 : Collections.<Object>emptySet());
            TestSupport.checkSet(this.changes2, set2 != null ? set2 : Collections.<Object>emptySet());
            TestSupport.checkSet(this.changes3, set3 != null ? set3 : Collections.<Object>emptySet());
            TestSupport.checkSet(this.changes4, set4 != null ? set4 : Collections.<Object>emptySet());
            TestSupport.checkSet(this.changes5, set5 != null ? set5 : Collections.<Object>emptySet());
            this.reset();
        }

        @PermazenField
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField
        public abstract int getAge();
        public abstract void setAge(int age);

        public abstract List<Person> getFriends();

    // Test changes reported

        @OnChange
        private void onChange1(FieldChange<?> change) {
            this.changes1.add(new Pair(change));
        }

        @OnChange
        private void onChange2(FieldChange<?> change) {
            this.changes2.add(new Pair(change));
        }

        @OnChange(path = "->friends")
        private void onChange3(FieldChange<?> change) {
            this.changes3.add(new Pair(change));
        }

        @OnChange(path = "->friends.element->friends")
        private void onChange4(FieldChange<?> change) {
            this.changes4.add(new Pair(change));
        }

        @OnChange("age")
        private void onChange5(SimpleFieldChange<Person, Integer> change) {
            this.changes5.add(new Pair(change));
        }

    // Test valid reference path + parameter type combinations

        @OnChange(path = "->friends")
        private void onChangeParameterTypeTest1(SimpleFieldChange<Person, ?> change) {
        }

        @OnChange(path = "->friends")
        private void onChangeParameterTypeTest2(ListFieldChange<Person> change) {
        }
    }

    @PermazenType
    public abstract static class Bogus1 implements PermazenObject {

        public abstract Person getFriend();
        public abstract void setFriend(Person friend);

    // Malformed path

        @OnChange(path = "?friend?")
        private void onChange() {
        }
    }

    @PermazenType
    public abstract static class Bogus2 implements PermazenObject {

        public abstract Map<Person, Integer> getFriends();

    // Invalid path missing sub-field

        @OnChange(path = "->friends")
        private void onChange() {
        }
    }

    @PermazenType
    public abstract static class Bogus3 implements PermazenObject {

        public abstract List<Person> getFriends();

    // Invalid reference path + parameter type combination due to no matching field

        @OnChange(path = "->friends")
        private void onChange(SetFieldChange<Person> change) {
        }
    }

    @PermazenType
    public abstract static class Bogus4 implements PermazenObject {

        public abstract List<Person> getFriends();

    // Invalid reference path + parameter type combination due to type erasure

        @OnChange(path = "->friends")
        private void onChange(SimpleFieldChange<Person, String> change) {
        }
    }

    @PermazenType
    public abstract static class Bogus5 implements PermazenObject {

        public abstract List<Person> getFriends();

    // Invalid target field specified

        @OnChange(path = "<-Bogus5.friends", value = "blob")
        private void onChange(FieldChange<?> change) {
        }
    }
}
