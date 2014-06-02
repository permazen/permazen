
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.ReadOnlyTransactionException;
import org.springframework.transaction.annotation.Transactional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SimpleSpringTest extends SpringTest {

    @Test
    public void testSpring() {
        final SimpleSpringTest bean = this.context.getBean(SimpleSpringTest.class);

        // Test normal transaction
        final Person p1 = bean.createPerson();

        // Test normal transaction
        bean.testSetName(p1, "Smith");

        // Test readback
        Assert.assertEquals(bean.testGetName(p1), "Smith");

        // Test access outside of transaction
        try {
            p1.getName();
            assert false;
        } catch (IllegalStateException e) {
            // expected
        }

        // Test read-only transaction
        try {
            bean.testSetNameReadOnly(p1, "foo");
            assert false;
        } catch (ReadOnlyTransactionException e) {
            // expected
        }
    }

    @Test
    public void testFilter() {
        final JSimpleDB db = this.context.getBean(JSimpleDB.class);
        Assert.assertTrue(db.getJClassesByStorageId().keySet().contains(100));
        Assert.assertFalse(db.getJClassesByStorageId().keySet().contains(200));
    }

// Bean methods

    @Transactional
    public Person createPerson() {
        return Person.create();
    }

    @Transactional
    public void testSetName(Person p1, String name) {
        p1.setName(name);
    }

    @Transactional(readOnly = true)
    public void testSetNameReadOnly(Person p1, String name) {
        p1.setName(name);
    }

    @Transactional(readOnly = true)
    public String testGetName(Person p1) {
        return p1.getName();
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        @JField(storageId = 109)
        public abstract String getName();
        public abstract void setName(String value);

        public static Person create() {
            return JTransaction.getCurrent().create(Person.class);
        }
    }

    @JSimpleClass(storageId = 200)
    public abstract static class Banana implements JObject {

        @JField(storageId = 201)
        public abstract float getWeight();
        public abstract void setWeight(float weight);
    }
}

