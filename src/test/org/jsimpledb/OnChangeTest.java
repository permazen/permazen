
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnChange;
import org.jsimpledb.change.FieldChange;
import org.jsimpledb.change.ListFieldAdd;
import org.jsimpledb.change.ListFieldChange;
import org.jsimpledb.change.ListFieldClear;
import org.jsimpledb.change.ListFieldRemove;
import org.jsimpledb.change.ListFieldReplace;
import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldChange;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.change.SetFieldAdd;
import org.jsimpledb.change.SetFieldChange;
import org.jsimpledb.change.SetFieldClear;
import org.jsimpledb.change.SetFieldRemove;
import org.jsimpledb.change.SimpleFieldChange;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OnChangeTest extends TestSupport {

    private static final ThreadLocal<ArrayList<FieldChange<?>>> EVENTS = new ThreadLocal<ArrayList<FieldChange<?>>>() {
        @Override
        protected ArrayList<FieldChange<?>> initialValue() {
            return new ArrayList<FieldChange<?>>();
        }
    };

    @Test
    public void testSimpleFieldChange() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class, MeanPerson.class, NicePerson.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person p1 = tx.create(Person.class);

            final NicePerson n1 = tx.create(NicePerson.class);
            final NicePerson n2 = tx.create(NicePerson.class);

            final MeanPerson m1 = tx.create(MeanPerson.class);

            this.verify();

            p1.setName("Person #1");
            this.verify(new SimpleFieldChange<Person, String>(p1, null, "Person #1"));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            p1.getKnownPeople().add(n1);
            this.verify(new ListFieldAdd<Person, Person>(p1, 0, n1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, 1, n2));

            p1.getKnownPeople().add(0, m1);
            this.verify(new ListFieldAdd<Person, Person>(p1, 0, m1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, 3, n2));

            p1.getKnownPeople().set(3, n2);     // no change
            this.verify();

            p1.getKnownPeople().set(3, p1);
            this.verify(new ListFieldReplace<Person, Person>(p1, 3, n2, p1));

            p1.getKnownPeople().remove(3);
            this.verify(new ListFieldRemove<Person, Person>(p1, 3, p1));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            m1.getEnemies().put(n1, 0.5f);
            this.verify(new MapFieldAdd<MeanPerson, NicePerson, Float>(m1, n1, 0.5f));

            // Now there exists a path p1.knownPeople.element -> m1.enemies.key -> n1
            n1.setAge(20);
            this.verify(new SimpleFieldChange<Person, Integer>(n1, 10, 20));

            m1.getEnemies().put(n1, 0.5f);      // no change
            this.verify();

            m1.getEnemies().put(n1, 2.5f);
            this.verify(new MapFieldReplace<MeanPerson, NicePerson, Float>(m1, n1, 0.5f, 2.5f));

            m1.getEnemies().remove(n2);
            this.verify();

            m1.getEnemies().remove(n1);
            this.verify(new MapFieldRemove<MeanPerson, NicePerson, Float>(m1, n1, 2.5f));

            m1.getEnemies().remove(n1);
            this.verify();

            m1.getEnemies().clear();        // map was already clear
            this.verify();

            m1.getEnemies().put(null, 2.5f);
            this.verify(new MapFieldAdd<MeanPerson, NicePerson, Float>(m1, null, 2.5f));

            m1.getEnemies().clear();
            this.verify(new MapFieldClear<MeanPerson>(m1));

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private void verify(FieldChange<?>... changes) {
        Assert.assertEquals(EVENTS.get(), Arrays.asList(changes));
        EVENTS.get().clear();
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        @JField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @JField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @JListField(storageId = 103, element = @JField(storageId = 104))
        public abstract List<Person> getKnownPeople();

        @OnChange("knownPeople.element.enemies.key.age")
        private void knownEnemyAgeChange(SimpleFieldChange<NicePerson, Integer> change) {
            EVENTS.get().add(change);
        }

    // name

        @OnChange("name")
        private void nameChange(FieldChange<Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("name")
        private void nameChange(SimpleFieldChange<Person, String> change) {
            Assert.assertSame(change.getObject(), this);
        }

    // knownPeople

        @OnChange("knownPeople")
        private void knownPeopleChange(FieldChange<Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldChange<Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldAdd<Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldRemove<Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldReplace<Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldClear<Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @JMapField(storageId = 201,
          key = @JField(storageId = 202),
          value = @JField(storageId = 203, type = "float"))
        public abstract Map<NicePerson, Float> getEnemies();

    // enemies

        @OnChange("enemies")
        private void enemiesChange(FieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldClear<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldAdd<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldRemove<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldReplace<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange(startType = Person.class, value = "name")
        private static void personNameChange(SimpleFieldChange<Person, String> change) {
            EVENTS.get().add(change);
        }
    }

    @JSimpleClass(storageId = 300)
    public abstract static class NicePerson extends Person {

        @JMapField(storageId = 301,
          key = @JField(storageId = 302),
          value = @JField(storageId = 303))
        public abstract Map<Person, Float> getRatings();

        @JSetField(storageId = 304, element = @JField(storageId = 305))
        public abstract Set<Person> getFriends();

    // ratings

        @OnChange("ratings")
        private void ratingsChange(FieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldAdd<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldRemove<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldReplace<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

    // friends

        @OnChange("friends")
        private void friendsChange(FieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldAdd<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldRemove<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            EVENTS.get().add(change);
        }
    }
}

