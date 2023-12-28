
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConstructorTest extends MainTestSupport {

    @Test
    public void testConstrutorWithParam() {
        final Permazen jdb = BasicTest.newPermazen(Person.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            jtx.create(Person.class);
            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testPrivateConstrutor() {
        try {
            BasicTest.newPermazen(Person2.class);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMissingConstrutor() {
        try {
            BasicTest.newPermazen(Person3.class);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements JObject {
        protected Person(JTransaction jtx, ObjId id) {
            Assert.assertEquals(jtx, this.getTransaction());
            Assert.assertEquals(id, this.getObjId());
        }
    }

    @PermazenType
    public abstract static class Person2 implements JObject {
        Person2(JTransaction jtx, ObjId id) {        // package private
        }
    }

    @PermazenType
    public abstract static class Person3 implements JObject {
        public Person3(int dummy) {
        }
    }
}
