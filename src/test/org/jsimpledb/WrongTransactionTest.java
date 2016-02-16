
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JSimpleClass;
import org.testng.annotations.Test;

public class WrongTransactionTest extends TestSupport {

    @Test
    public void testWrongTransaction() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);

        final Person p1;

        // Tx #1
        JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {
            p1 = (Person)tx.create(Person.class).copyOut();
        } finally {
            tx.commit();
            JTransaction.setCurrent(null);
        }

        // Tx #2
        tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {
            final Person p2 = tx.create(Person.class);
            try {
                p2.setFriend(p1);
                assert false : "allowed setting reference to an object in a different transaction";
            } catch (IllegalArgumentException e) {
                // expected
            }
        } finally {
            tx.commit();
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        public abstract Person getFriend();
        public abstract void setFriend(Person person);
    }
}

