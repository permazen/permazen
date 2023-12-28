
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.PermazenType;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JObjectCopyTest extends MainTestSupport {

    @Test
    public void testCopyWithPath() throws Exception {
        final Permazen jdb = BasicTest.newPermazen(Person.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        final DetachedJTransaction stx = jtx.getDetachedTransaction();
        JTransaction.setCurrent(jtx);
        try {

            // Root person
            final Person p1 = jtx.create(Person.class);

            // Intermediate people
            final Person p2a = jtx.create(Person.class);
            final Person p2b = jtx.create(Person.class);

            // Target person
            final Person p3 = jtx.create(Person.class);

            // Set up graph of references
            p1.getFriends().add(p2a);
            p1.getFriends().add(p2b);
            p2b.getFriends().add(p3);

            // Copy out
            p1.copyOut("friends");

            // Verify p3 got copied out
            Assert.assertTrue(stx.get(p3).exists());

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopyMultiplePath() throws Exception {
        final Permazen jdb = BasicTest.newPermazen(Person.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        final DetachedJTransaction stx = jtx.getDetachedTransaction();
        JTransaction.setCurrent(jtx);
        try {
            final Person p1 = jtx.create(Person.class);
            final Person p2 = jtx.create(Person.class);
            final Person p3 = jtx.create(Person.class);

            // Set up graph of references
            p1.setRef(p2);
            p2.setRef(p3);

            // Copy out
            p1.copyOut("ref", "ref");

            // Verify p3 got copied out
            Assert.assertTrue(stx.get(p3).exists());

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements JObject {

        @JField(forwardCascades = "ref")
        public abstract Person getRef();
        public abstract void setRef(Person ref);

        @JListField(element = @JField(forwardCascades = "friends"))
        public abstract List<Person> getFriends();

        @Override
        public String toString() {
            return "Person@" + this.getObjId();
        }
    }
}
