
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
        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {
            ptx.create(Person.class);
            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
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
    public abstract static class Person implements PermazenObject {
        @SuppressWarnings("this-escape")
        protected Person(PermazenTransaction ptx, ObjId id) {
            Assert.assertEquals(ptx, this.getPermazenTransaction());
            Assert.assertEquals(id, this.getObjId());
        }
    }

    @PermazenType
    public abstract static class Person2 implements PermazenObject {
        Person2(PermazenTransaction ptx, ObjId id) {        // package private
        }
    }

    @PermazenType
    public abstract static class Person3 implements PermazenObject {
        public Person3(int dummy) {
        }
    }
}
