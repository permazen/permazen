
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnCreate;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OnCreateTest extends TestSupport {

    static boolean createFriend;

    @Test
    public void testOnCreate() {

        final JLayer jlayer = JLayerTest.getJLayer(Person.class);
        final JTransaction tx = jlayer.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
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
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public static class Person {

        private int createInvokes;
        private Person friend;

        @OnCreate
        private void beingCreated() {
            this.createInvokes++;
            if (OnCreateTest.createFriend) {
                OnCreateTest.createFriend = false;
                this.setFriend(JTransaction.getCurrent().create(Person.class));
            }
        }

        public int getCreateInvokes() {
            return this.createInvokes;
        }

        @JField(storageId = 101)
        public Person getFriend() {
            return this.friend;
        }
        public void setFriend(Person friend) {
            this.friend = friend;
        }
    }
}

