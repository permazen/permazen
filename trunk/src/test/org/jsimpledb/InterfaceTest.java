
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.annotation.IndexQuery;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.testng.annotations.Test;

public class InterfaceTest extends TestSupport {

    @Test
    public void testInterface() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class, Dog.class, Cat.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person owner = tx.create(Person.class);

            final Dog dog = tx.create(Dog.class);
            final Cat cat = tx.create(Cat.class);

            owner.getPets().add(dog);
            owner.getPets().add(cat);

            dog.setFriend(cat);
            cat.setFriend(dog);

            cat.setEnemy(dog);

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
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

        @JSetField(storageId = 120, element = @JField(storageId = 121, indexed = true))
        public abstract Set<Pet> getPets();

        @IndexQuery(startType = Pet.class, value = "enemy")
        public abstract NavigableMap<Dog, NavigableSet<Pet>> queryEnemies();

        @IndexQuery("pets.element")
        public abstract NavigableMap<Pet, NavigableSet<Person>> queryPets();
    }

    @JSimpleClass(storageId = 200)
    public abstract static class Dog implements Pet {
    }

    @JSimpleClass(storageId = 300)
    public abstract static class Cat implements Pet {

        @JField(storageId = 301)
        public abstract Dog getEnemy();
        public abstract void setEnemy(Dog enemy);

        @IndexQuery("friend")
        public abstract NavigableMap<Pet, NavigableSet<Cat>> queryFriend();
    }

    public interface Pet extends JObject {

        @JField(storageId = 201)
        Pet getFriend();
        void setFriend(Pet pet);
    }
}

