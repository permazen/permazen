
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnDelete;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnDeleteTest2 extends MainTestSupport {

    private static final ThreadLocal<HashSet<Delete>> DELETES = ThreadLocal.withInitial(HashSet::new);

    private Set<Delete> expectedDeletes;

    private record Delete(String methodName, String deleted, String target) {

        // For self notifications
        Delete(String methodName, Animal deleted) {
            this(methodName, deleted, deleted);
        }

        // For general notifications
        Delete(String methodName, Animal deleted, Animal target) {
            this(methodName, deleted.getName(), target.getName());
        }
    }

    void verifyExpectedDeletes() {
        Assert.assertEquals(DELETES.get(), this.expectedDeletes);
        this.log.debug("verified deletes: {}", this.expectedDeletes);
        DELETES.get().clear();
    }

    @Test
    public void testOnDelete2() {

        final Permazen pdb = BasicTest.newPermazen(
          Lion.class,
          Tiger.class,
          Bear.class);

        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            // Setup objects
            final Lion lion1 = tx.create(Lion.class);
            final Lion lion2 = tx.create(Lion.class);
            final Tiger tiger1 = tx.create(Tiger.class);
            final Tiger tiger2 = tx.create(Tiger.class);
            final Bear bear1 = tx.create(Bear.class);
            final Bear bear2 = tx.create(Bear.class);

            lion1.setName("lion1");
            lion2.setName("lion2");
            tiger1.setName("tiger1");
            tiger2.setName("tiger2");
            bear1.setName("bear1");
            bear2.setName("bear2");

        /*

            lion1 <--[cat]--> lion2 <--[animal]-- bear1
              |                 |
            [animal]         [animal]
              |                 |
              v                 v
            tiger1            bear2

            tiger2 <--
              |       \
              |     [cat]
               \______/

        */

            lion1.setCatFriend(lion2);
            lion1.setAnimalFriend(tiger1);

            lion2.setCatFriend(lion1);
            lion2.setAnimalFriend(bear2);

            bear1.setAnimalFriend(lion2);

            tiger2.setCatFriend(tiger2);

            // Now test notifications

            this.expectedDeletes = Set.of();
            this.verifyExpectedDeletes();

            // Delete tiger1
            this.expectedDeletes = Set.of(
              new Delete("onCatDelete", tiger1)
            );
            tiger1.delete();
            this.verifyExpectedDeletes();

            // Delete lion1
            this.expectedDeletes = Set.of(
              new Delete("onCatDelete", lion1),
              new Delete("onAnimalCatDelete", lion1, bear1)
            );
            lion1.delete();
            this.verifyExpectedDeletes();

            // Delete tiger2
            this.expectedDeletes = Set.of(
              new Delete("onCatDelete", tiger2)
              //new Delete("onTigerFriendDelete", tiger2)       // this is redundant and has longer path, so it should not fire
            );
            tiger2.delete();
            this.verifyExpectedDeletes();

            // Delete lion2
            this.expectedDeletes = Set.of(
              new Delete("onCatDelete", lion2),
              new Delete("onLionAnimalFriendDelete", lion2, bear2)
            );
            lion2.delete();
            this.verifyExpectedDeletes();

            // Delete bear1
            this.expectedDeletes = Set.of();
            bear1.delete();
            this.verifyExpectedDeletes();

            // Delete bear2
            this.expectedDeletes = Set.of();
            bear2.delete();
            this.verifyExpectedDeletes();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInvalids() {
        for (Class<?> invalidClass : List.of(
          Invalid1.class, Invalid2.class, Invalid3.class, Invalid4.class, Invalid5.class, Invalid6.class)) {
            try {
                BasicTest.newPermazen(Lion.class, Tiger.class, Bear.class, invalidClass).initialize();
                assert false : "expected an error with " + invalidClass;
            } catch (IllegalArgumentException e) {
                this.log.debug("got expected " + e);
            }
        }
    }

// Model Classes

    public interface Animal extends PermazenObject {

        String getName();
        void setName(String name);

        @PermazenField(inverseDelete = DeleteAction.NULLIFY)
        Animal getAnimalFriend();
        void setAnimalFriend(Animal animal);

        @OnDelete(path = "->animalFriend->catFriend")
        default void onAnimalCatDelete(Cat cat) {
            DELETES.get().add(new Delete("onAnimalCatDelete", cat, this));
        }
    }

    public interface Cat extends Animal {

        @PermazenField(inverseDelete = DeleteAction.NULLIFY)
        Cat getCatFriend();
        void setCatFriend(Cat cat);

        @OnDelete
        default void onCatDelete() {
            DELETES.get().add(new Delete("onCatDelete", this));
        }
    }

    @PermazenType
    public interface Lion extends Cat {
    }

    @PermazenType
    public interface Tiger extends Cat {

        @OnDelete(path = "->animalFriend")
        default void onTigerFriendDelete(Tiger tiger) {
            DELETES.get().add(new Delete("onTigerFriendDelete", tiger, this));
        }
    }

    @PermazenType
    public interface Bear extends Animal {

        @OnDelete(path = "<-Lion.animalFriend")
        default void onLionAnimalFriendDelete(Cat cat) {
            DELETES.get().add(new Delete("onLionAnimalFriendDelete", cat, this));
        }
    }

// Invalid examples

    @PermazenType
    public interface Invalid1 extends Animal {

        @OnDelete
        default void onDelete(Bear bear) {
            // this method should not have a parameter
        }
    }

    @PermazenType
    public interface Invalid2 extends Animal {

        @OnDelete(path = "->animalFriend")
        default void onDelete() {
            // this method should have one parameter
        }
    }

    @PermazenType
    public interface Invalid3 extends Animal {

        @OnDelete(path = "<-Tiger.catFriend")
        default void onDelete(Tiger tiger) {
            // invalid path - this class can't be a Cat friend
        }
    }

    @PermazenType
    public interface Invalid4 extends Animal {

        @OnDelete(path = "->animalFriend->catFriend")
        default void onDelete(Bear bear) {
            // invalid parameter type - a Cat friend can't be a Bear
        }
    }

    @PermazenType
    public interface Invalid5 extends Animal {

        @OnDelete
        static void onDelete() {
            // this method should have one parameter
        }
    }

    @PermazenType
    public interface Invalid6 extends Animal {

        @OnDelete
        static void onDelete(Integer x) {
            // invalid parameter type
        }
    }
}
