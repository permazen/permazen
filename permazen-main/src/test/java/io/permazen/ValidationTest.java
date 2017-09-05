
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.annotation.OnValidate;
import io.permazen.test.TestSupport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

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

            // Reload object
            int checks = p1.getChecks();
            p1 = tx.get(p1);
            p1.setChecks(checks);

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

            // Reload object
            int checks = p1.getChecks();
            p1 = tx.get(p1);
            p1.setChecks(checks);

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

            // Reload object
            int checks = p1.getChecks();
            p1 = tx.get(p1);
            p1.setChecks(checks);

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

        // Now test @OnValidate
        tx = jdb.createTransaction(false, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            // Reload object
            int checks = p1.getChecks();
            p1 = tx.get(p1);
            p1.setChecks(checks);

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

            // Reload object
            int checks = p1.getChecks();
            p1 = tx.get(p1);
            p1.setChecks(checks);

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

    @Test
    public void testNameThing1() {
        this.testNameThing(NameThing1.class);
    }

    @Test
    public void testNameThing2() {
        this.testNameThing(NameThing2.class);
    }

    @Test
    public void testNameThing3() {
        this.testNameThing(NameThing3.class);
    }

    @Test
    public void testNameThing4() {
        this.testNameThing(NameThing4.class);
    }

    @Test
    public void testNameThing5() {
        this.testNameThing(NameThing5.class);
    }

    @Test
    public void testNameThing6() {
        this.testNameThing(NameThing5.class);
    }

    private <T extends NameThing> void testNameThing(Class<T> type) {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(type);

        // Transaction with validation enabled
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            final NameThing nameThing = jtx.create(type);
            nameThing.setName("");
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

        @OnValidate
        private void checkMe() {
            if (this.checks == -1)
                throw new ValidationException(this, "checks == -1");
            this.checks++;
        }
    }

    public abstract static class NameThing implements JObject {

        public abstract String getName();
        public abstract void setName(String name);

        @Override
        public String toString() {
            return this.getName();
        }
    }

// This class has only a type-level JSR 303 constraint

    @JSimpleClass
    @NonEmptyToString
    public abstract static class NameThing1 extends NameThing {
    }

// This class has only a @OnValidate method

    @JSimpleClass
    public abstract static class NameThing2 extends NameThing {
        @OnValidate
        public void validateMe() {
            throw new ValidationException(this, "sorry");
        }
    }

// This class extends a class with only a type-level JSR 303 constraint

    @JSimpleClass
    public abstract static class NameThing3 extends NameThing1 {
    }

// This class implements an interface with a method having a JSR 303 constraint

    public interface Foobar {
        @NotNull
        Object getFoo();
    }

    @JSimpleClass
    public abstract static class NameThing4 extends NameThing implements Foobar {
        public Object getFoo() {
            return null;
        }
    }

// This class implements an interface with a method having a @OnValidate constraint

    public interface ValidateMe {
        @OnValidate
        void validateMe();
    }

    @JSimpleClass
    public abstract static class NameThing5 extends NameThing implements ValidateMe {
        public void validateMe() {
            throw new ValidationException(this, "sorry");
        }
    }

// This class extends a class with only a @OnValidate method

    @JSimpleClass
    public abstract static class NameThing6 extends NameThing2 {
        @Override
        public void validateMe() {
            super.validateMe();
        }
    }

// NonEmptyToString

    @Constraint(validatedBy = NonEmptyToStringValidator.class)
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface NonEmptyToString {
        String message() default "toString() returnend an empty string";
        Class<?>[] groups() default {};
        Class<? extends Payload>[] payload() default {};
    }

    public static class NonEmptyToStringValidator implements ConstraintValidator<NonEmptyToString, Object> {

        @Override
        public void initialize(NonEmptyToString annotation) {
        }

        @Override
        public boolean isValid(Object obj, ConstraintValidatorContext context) {
            return obj == null || obj.toString().length() > 0;
        }
    }
}

