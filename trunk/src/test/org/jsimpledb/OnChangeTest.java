
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
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldReplace;
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

        final JLayer jlayer = JLayerTest.getJLayer(Person.class, MeanPerson.class, NicePerson.class);
        final JTransaction tx = jlayer.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person p1 = tx.create(Person.class);

            final NicePerson n1 = tx.create(NicePerson.class);
            final NicePerson n2 = tx.create(NicePerson.class);

            final MeanPerson m1 = tx.create(MeanPerson.class);

            this.verify();

            p1.setName("Person #1");

            this.verify(new SimpleFieldChange<Person, String>(p1, null, "Person #1"));

            n1.setAge(10);

            this.verify();

            p1.getKnownPeople().add(n1);
            p1.getKnownPeople().add(n2);
            p1.getKnownPeople().add(m1);

            m1.getEnemies().put(n1, 0.5f);

            this.verify();

            n1.setAge(10);

            this.verify();

            n1.setAge(20);

            this.verify(new SimpleFieldChange<Person, Integer>(n1, 10, 20));

            m1.getEnemies().put(n1, 1.5f);

            this.verify(new MapFieldReplace<MeanPerson, NicePerson, Float>(m1, n1, 0.5f, 1.5f));

            m1.getEnemies().clear();

            this.verify(new MapFieldClear<MeanPerson, NicePerson, Float>(m1));

            n1.setAge(30);

            this.verify();

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
    }

    @JSimpleClass(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @JMapField(storageId = 201,
          key = @JField(storageId = 202),
          value = @JField(storageId = 203, type = "float"))
        public abstract Map<NicePerson, Float> getEnemies();

        @OnChange("enemies")
        private void enemiesClear(MapFieldClear<MeanPerson, NicePerson, Float> change) {
            EVENTS.get().add(change);
        }

        @OnChange("enemies")
        private void enemiesReplace(MapFieldReplace<MeanPerson, NicePerson, Float> change) {
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
    }
}

