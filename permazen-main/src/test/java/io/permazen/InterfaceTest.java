
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.index.Index;
import io.permazen.schema.NameIndex;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import jakarta.validation.constraints.NotNull;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class InterfaceTest extends TestSupport {

    @Test
    public void testInterface() {

        final Permazen jdb = BasicTest.getPermazen(Person.class, Dog.class, Cat.class);
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

            Person.queryEnemies();
            Person.queryPets();
            Cat.queryFriend();

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInterfaceModelClasses() {

        final Permazen jdb = BasicTest.getPermazen(Human.class, NutriaRat.class, ContainerClass.OwnedPet.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Human fred = jtx.create(Human.class);

            final NutriaRat poncho = jtx.create(NutriaRat.class);
            final NutriaRat lefty = jtx.create(NutriaRat.class);

            poncho.setFriend(lefty);
            poncho.setOwner(fred);

            lefty.setFriend(poncho);
            lefty.setOwner(fred);

            jtx.create(ContainerClass.OwnedPet.class);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDoubleInherit() {

        final Permazen jdb = BasicTest.getPermazen(Foo.class, Bar.class);

        final SchemaModel schema = jdb.getSchemaModel();
        final NameIndex lookup = new NameIndex(schema);
        final ReferenceSchemaField fooField = (ReferenceSchemaField)lookup.getSchemaField(lookup.getSchemaObjectType("Bar"), "foo");

        Assert.assertEquals(fooField.getOnDelete(), DeleteAction.DELETE);
    }

// Model Classes #1

    @PermazenType
    public abstract static class Person implements JObject {

        public abstract String getName();
        public abstract void setName(String name);

        public abstract int getAge();
        public abstract void setAge(int age);

        @JSetField(element = @JField(indexed = true))
        public abstract Set<Pet> getPets();

        public static Index<Dog, Pet> queryEnemies() {
            return JTransaction.getCurrent().queryIndex(Pet.class, "enemy", Dog.class);
        }

        public static Index<Pet, Person> queryPets() {
            return JTransaction.getCurrent().queryIndex(Person.class, "pets.element", Pet.class);
        }
    }

    @PermazenType
    public abstract static class Dog implements Pet {
    }

    @PermazenType
    public abstract static class Cat implements Pet {

        public abstract Dog getEnemy();
        public abstract void setEnemy(Dog enemy);

        public static Index<Pet, Cat> queryFriend() {
            return JTransaction.getCurrent().queryIndex(Cat.class, "friend", Pet.class);
        }
    }

    public interface Pet extends JObject {

        Pet getFriend();
        void setFriend(Pet pet);
    }

// Model Classes #2

    @PermazenType
    public interface Animal {
        int getNumLegs();
        void setNumLegs(int numLegs);
    }

    public interface CanReason {
        float getIQ();
        void setIQ(float iq);
    }

    public static class ContainerClass {

        @PermazenType
        public interface OwnedPet extends Pet {

            Human getOwner();
            void setOwner(Human owner);
        }
    }

    @PermazenType
    public interface Human extends Animal, CanReason {
    }

    @PermazenType
    public abstract static class NutriaRat implements Animal, ContainerClass.OwnedPet {
    }

// Model Classes #3

    @PermazenType
    public abstract static class Foo {
    }

    public interface HasOptionalFoo {
        @JField(onDelete = DeleteAction.DELETE)
        Foo getFoo();
        void setFoo(Foo x);
    }

    public interface HasFoo extends HasOptionalFoo {
        @NotNull
        @Override
        Foo getFoo();
    }

    @PermazenType
    public abstract static class Bar implements HasFoo {
        @Override
        public abstract Foo getFoo();
    }
}

