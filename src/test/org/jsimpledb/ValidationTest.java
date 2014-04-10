
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.List;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JSimpleClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidationTest extends TestSupport {

    @Test
    public void testValidation() {

        final JLayer jlayer = JLayerTest.getJLayer(Person.class);

        // Transaction with validation disabled
        JTransaction tx = jlayer.createTransaction(true, ValidationMode.DISABLED);
        JTransaction.setCurrent(tx);
        Person p1;
        try {
            p1 = tx.create(Person.class);

            p1.setName(null);
            p1.setAge(-10);
            p1.getFriends().add(null);
            p1.getFriends().add(null);
            p1.getFriends().add(null);

            // Attempt validation - request should be ignored
            p1.revalidate();
            try {
                tx.validate();
            } catch (ValidationException e) {
                assert false;
            }

            tx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        // Transaction with validation manually requested
        tx = jlayer.createTransaction(false, ValidationMode.MANUAL);
        JTransaction.setCurrent(tx);
        try {

            // Request validation
            p1.revalidate();

            // Verify same invalid stuff
            Assert.assertEquals(p1.getAge(), -10);
            Assert.assertEquals(p1.getFriends().size(), 3);

            // Making an invalid change has no immediate effect
            p1.setAge(-20);

            // Now validate
            try {
                tx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
            }

            // Make p1 valid
            p1.setName("fred");
            p1.setAge(32);
            p1.getFriends().clear();
            try {
                tx.validate();
            } catch (ValidationException e) {
                assert false;
            }

            // No revalidation requested yet...
            p1.setName(null);
            try {
                tx.validate();
            } catch (ValidationException e) {
                assert false;
            }

            // OK now
            p1.revalidate();
            try {
                tx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
            }

            // Leave invalid for commit - should be allowed

            tx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        // Transaction with automatic validation
        tx = jlayer.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setAge(40);
            try {
                tx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
            }

        } finally {
            JTransaction.setCurrent(null);
        }

        // Now fix the problem
        tx = jlayer.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setName("bob");
            try {
                tx.commit();
            } catch (ValidationException e) {
                assert false;
            }

        } finally {
            JTransaction.setCurrent(null);
        }

    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        @JField(storageId = 101)
        @NotNull
        public abstract String getName();
        public abstract void setName(String name);

        @JField(storageId = 102)
        @Min(0)
        public abstract int getAge();
        public abstract void setAge(int age);

        @JListField(storageId = 103, element = @JField(storageId = 104))
        @NotNull
        @Size(max = 2)
        public abstract List<Person> getFriends();
    }
}

