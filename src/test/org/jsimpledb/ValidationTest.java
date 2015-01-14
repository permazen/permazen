
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
import org.jsimpledb.annotation.Validate;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidationTest extends TestSupport {

    @Test
    public void testValidation() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);

        // Transaction with validation disabled
        JTransaction tx = jdb.createTransaction(true, ValidationMode.DISABLED);
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

            Assert.assertEquals(p1.getChecks(), 0);

            tx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        // Transaction with validation manually requested
        tx = jdb.createTransaction(false, ValidationMode.MANUAL);
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
                Assert.assertEquals(e.getViolations().size(), 3);
            }

            Assert.assertEquals(p1.getChecks(), 0);

            // Make p1 valid, request revalidation, and validate
            p1.setName("fred");
            p1.setAge(32);
            p1.getFriends().clear();
            p1.revalidate();
            try {
                tx.validate();
            } catch (ValidationException e) {
                assert false;
            }

            Assert.assertEquals(p1.getChecks(), 1);

            // No revalidation requested yet...
            p1.setName(null);
            try {
                tx.validate();
            } catch (ValidationException e) {
                assert false;
            }

            Assert.assertEquals(p1.getChecks(), 1);

            // OK now
            p1.revalidate();
            try {
                tx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
                Assert.assertEquals(e.getViolations().size(), 1);
            }

            Assert.assertEquals(p1.getChecks(), 1);

            // Leave invalid for commit - should be allowed

            tx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }

        // Transaction with automatic validation
        tx = jdb.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setAge(40);
            try {
                tx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
                Assert.assertEquals(e.getViolations().size(), 1);
            }

            Assert.assertEquals(p1.getChecks(), 1);

        } finally {
            JTransaction.setCurrent(null);
        }

        // Now fix the problem
        tx = jdb.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setName("bob");
            try {
                tx.commit();
            } catch (ValidationException e) {
                assert false;
            }

            Assert.assertEquals(p1.getChecks(), 2);

        } finally {
            JTransaction.setCurrent(null);
        }

        // Now test @Validate
        tx = jdb.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setChecks(-1);
            p1.revalidate();

            try {
                tx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
                Assert.assertEquals(e.getObject(), p1);
                Assert.assertNull(e.getViolations());
            }

        } finally {
            JTransaction.setCurrent(null);
        }

        // Test clearing validation queue
        tx = jdb.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            p1.setChecks(-1);
            p1.setAge(-99);
            p1.revalidate();

            // Reset validation queue
            tx.resetValidationQueue();

            // Attempt validation - request should be ignored
            tx.validate();

            tx.rollback();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testValidationOnCreate() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);

        // Transaction with validation disabled
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            jtx.create(Person.class);           // a newly created person is invalid due to having a null name
            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        private int checks;

        public int getChecks() {
            return this.checks;
        }
        public void setChecks(int checks) {
            this.checks = checks;
        }

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

        @Validate
        private void checkMe() {
            if (this.checks == -1)
                throw new ValidationException(this, "checks == -1");
            this.checks++;
        }
    }
}

