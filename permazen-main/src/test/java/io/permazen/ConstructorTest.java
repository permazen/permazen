
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JSimpleClass;
import io.permazen.core.ObjId;
import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConstructorTest extends TestSupport {

    @Test
    public void testConstrutorWithParam() {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
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
            BasicTest.getJSimpleDB(Person2.class);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMissingConstrutor() {
        try {
            BasicTest.getJSimpleDB(Person3.class);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {
        protected Person(JTransaction jtx, ObjId id) {
            Assert.assertEquals(jtx, this.getTransaction());
            Assert.assertEquals(id, this.getObjId());
        }
    }

    @JSimpleClass
    public abstract static class Person2 implements JObject {
        Person2(JTransaction jtx, ObjId id) {        // package private
        }
    }

    @JSimpleClass
    public abstract static class Person3 implements JObject {
        public Person3(int dummy) {
        }
    }
}

