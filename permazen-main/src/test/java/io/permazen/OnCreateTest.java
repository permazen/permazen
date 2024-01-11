
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnCreate;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnCreateTest extends MainTestSupport {

    static boolean createFriend;

    @Test
    public void testOnCreate1() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            Person p0 = new Person();
            Assert.assertEquals(p0.createInvokes, 0);

            Person p1 = tx.create(Person.class);
            Assert.assertEquals(p1.getCreateInvokes(), 1);
            Assert.assertNull(p1.getFriend());

            OnCreateTest.createFriend = true;
            Person p2 = tx.create(Person.class);
            Person p3 = p2.getFriend();
            Assert.assertNotNull(p3);
            Assert.assertEquals(p2.getCreateInvokes(), 1);
            Assert.assertEquals(p3.getCreateInvokes(), 1);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testOnCreate2() {

        final Permazen pdb = BasicTest.newPermazen(HasUUID.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            HasUUID h1 = tx.create(HasUUID.class);
            Assert.assertNotNull(h1.getUUID());

            tx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public static class Person {

        private int createInvokes;
        private Person friend;

        @OnCreate
        private void beingCreated() {
            this.createInvokes++;
            if (OnCreateTest.createFriend) {
                OnCreateTest.createFriend = false;
                this.setFriend(PermazenTransaction.getCurrent().create(Person.class));
            }
        }

        public int getCreateInvokes() {
            return this.createInvokes;
        }

        @PermazenField(storageId = 101)
        public Person getFriend() {
            return this.friend;
        }
        public void setFriend(Person friend) {
            this.friend = friend;
        }
    }

    @PermazenType(storageId = 100)
    public abstract static class HasUUID {

        @PermazenField(storageId = 101)
        @NotNull
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);

        @OnCreate
        private void initialize() {
            this.setUUID(UUID.randomUUID());
        }
    }
}
