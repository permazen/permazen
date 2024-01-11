
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenType;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PermazenObjectCopyTest extends MainTestSupport {

    @Test
    public void testCopyWithPath() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        final DetachedPermazenTransaction stx = ptx.getDetachedTransaction();
        PermazenTransaction.setCurrent(ptx);
        try {

            // Root person
            final Person p1 = ptx.create(Person.class);

            // Intermediate people
            final Person p2a = ptx.create(Person.class);
            final Person p2b = ptx.create(Person.class);

            // Target person
            final Person p3 = ptx.create(Person.class);

            // Set up graph of references
            p1.getFriends().add(p2a);
            p1.getFriends().add(p2b);
            p2b.getFriends().add(p3);

            // Copy out
            p1.copyOut("friends");

            // Verify p3 got copied out
            Assert.assertTrue(stx.get(p3).exists());

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopyMultiplePath() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        final DetachedPermazenTransaction stx = ptx.getDetachedTransaction();
        PermazenTransaction.setCurrent(ptx);
        try {
            final Person p1 = ptx.create(Person.class);
            final Person p2 = ptx.create(Person.class);
            final Person p3 = ptx.create(Person.class);

            // Set up graph of references
            p1.setRef(p2);
            p2.setRef(p3);

            // Copy out
            p1.copyOut("ref", "ref");

            // Verify p3 got copied out
            Assert.assertTrue(stx.get(p3).exists());

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        @PermazenField(forwardCascades = "ref")
        public abstract Person getRef();
        public abstract void setRef(Person ref);

        @PermazenListField(element = @PermazenField(forwardCascades = "friends"))
        public abstract List<Person> getFriends();

        @Override
        public String toString() {
            return "Person@" + this.getObjId();
        }
    }
}
