
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.List;

import org.jsimpledb.annotation.JSimpleClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JObjectCopyTest extends TestSupport {

    @Test
    public void testCopyMultiplePath() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final Person p3;
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        final SnapshotJTransaction stx = jtx.getSnapshotTransaction();
        JTransaction.setCurrent(jtx);
        try {

            // Root person
            final Person p1 = jtx.create(Person.class);

            // Intermediate people
            final Person p2a = jtx.create(Person.class);
            final Person p2b = jtx.create(Person.class);

            // Target person
            p3 = jtx.create(Person.class);

            // Set up graph of references
            p1.getFriends().add(p2a);
            p1.getFriends().add(p2b);
            p2b.getFriends().add(p3);

            // Copy out
            p1.copyOut("friends.element.friends.element");

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        // Verify p3 got copied out
        Assert.assertTrue(stx.getJObject(p3).exists());
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        public abstract List<Person> getFriends();

        @Override
        public String toString() {
            return "Person@" + this.getObjId();
        }
    }
}

