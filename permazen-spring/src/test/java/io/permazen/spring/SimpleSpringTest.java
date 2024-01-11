
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import io.permazen.Permazen;
import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.StaleTransactionException;

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
        } catch (StaleTransactionException e) {
            // expected
        }

        // Test read-only transaction
        bean.testSetNameReadOnly(p1, "SomeOtherNameBesidesSmith");
        Assert.assertEquals(bean.testGetName(p1), "Smith");
    }

    @Test
    public void testFilter() {
        final Permazen db = this.context.getBean(Permazen.class);
        Assert.assertTrue(db.getPermazenClassesByName().keySet().contains("Person"));
        Assert.assertFalse(db.getPermazenClassesByName().keySet().contains("Banana"));
    }

// Bean methods

    @Transactional
    public Person createPerson() {
        return Person.create();
    }

    @Transactional
    public void testSetName(Person p1, String name) {
        p1 = PermazenTransaction.getCurrent().get(p1);
        p1.setName(name);
    }

    @Transactional(readOnly = true)
    public void testSetNameReadOnly(Person p1, String name) {
        p1 = PermazenTransaction.getCurrent().get(p1);
        p1.setName(name);
    }

    @Transactional(readOnly = true)
    public String testGetName(Person p1) {
        p1 = PermazenTransaction.getCurrent().get(p1);
        return p1.getName();
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        @PermazenField
        public abstract String getName();
        public abstract void setName(String value);

        public static Person create() {
            return PermazenTransaction.getCurrent().create(Person.class);
        }
    }

    @PermazenType
    public abstract static class Banana implements PermazenObject {

        @PermazenField
        public abstract float getWeight();
        public abstract void setWeight(float weight);
    }
}
