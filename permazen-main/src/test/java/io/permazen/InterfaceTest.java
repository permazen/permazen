
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.index.Index1;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.schema.SchemaModel;

import jakarta.validation.constraints.NotNull;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class InterfaceTest extends MainTestSupport {

    @Test
    public void testInterface() {

        final Permazen pdb = BasicTest.newPermazen(Person.class, Dog.class, Cat.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInterfaceModelClasses() {

        final Permazen pdb = BasicTest.newPermazen(Human.class, NutriaRat.class, ContainerClass.OwnedPet.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Human fred = ptx.create(Human.class);

            final NutriaRat poncho = ptx.create(NutriaRat.class);
            final NutriaRat lefty = ptx.create(NutriaRat.class);

            poncho.setFriend(lefty);
            poncho.setOwner(fred);

            lefty.setFriend(poncho);
            lefty.setOwner(fred);

            ptx.create(ContainerClass.OwnedPet.class);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDoubleInherit() {

        final Permazen pdb = BasicTest.newPermazen(Foo.class, Bar.class);

        final SchemaModel schema = pdb.getSchemaModel();
        final ReferenceSchemaField fooField = (ReferenceSchemaField)schema.getSchemaObjectTypes()
          .get("Bar").getSchemaFields().get("foo");

        Assert.assertEquals(fooField.getInverseDelete(), DeleteAction.DELETE);
    }

// Model Classes #1

    @PermazenType
    public abstract static class Person implements PermazenObject {

        public abstract String getName();
        public abstract void setName(String name);

        public abstract int getAge();
        public abstract void setAge(int age);

        @PermazenSetField(element = @PermazenField(indexed = true))
        public abstract Set<Pet> getPets();

        public static Index1<Dog, Pet> queryEnemies() {
            return PermazenTransaction.getCurrent().querySimpleIndex(Pet.class, "enemy", Dog.class);
        }

        public static Index1<Pet, Person> queryPets() {
            return PermazenTransaction.getCurrent().querySimpleIndex(Person.class, "pets.element", Pet.class);
        }
    }

    @PermazenType
    public abstract static class Dog implements Pet {
    }

    @PermazenType
    public abstract static class Cat implements Pet {

        public abstract Dog getEnemy();
        public abstract void setEnemy(Dog enemy);

        public static Index1<Pet, Cat> queryFriend() {
            return PermazenTransaction.getCurrent().querySimpleIndex(Cat.class, "friend", Pet.class);
        }
    }

    public interface Pet extends PermazenObject {

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
        @PermazenField(inverseDelete = DeleteAction.DELETE)
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
