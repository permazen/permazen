
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnValidate;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenType;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidationTest extends MainTestSupport {

    @Test
    public void testValidation() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);

        // Transaction with validation disabled
        PermazenTransaction tx = pdb.createTransaction(ValidationMode.DISABLED);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }

        // Transaction with validation manually requested
        tx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }

        // Transaction with automatic validation
        tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }

        // Now fix the problem
        tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }

        // Now test @OnValidate
        tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }

        // Test clearing validation queue
        tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testValidationOnCreate() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);

        // Transaction with validation disabled
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {
            ptx.create(Person.class);           // a newly created person is invalid due to having a null name
            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
            }
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testEarlyLateValidation() {

        final Permazen pdb = BasicTest.newPermazen(EarlyValidation.class, LateValidation.class);

        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            EarlyValidation early = ptx.create(EarlyValidation.class);

            ptx.validate();

            assert early.getName().equals("placeholder");

            LateValidation late = ptx.create(LateValidation.class);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.info("got expected " + e);
            }

            late.setName("foo");

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
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

        final Permazen pdb = BasicTest.newPermazen(type);

        // Transaction with validation enabled
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {
            final NameThing nameThing = ptx.create(type);
            nameThing.setName("");
            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                // expected
            }
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements PermazenObject {

        private int checks;

        public int getChecks() {
            return this.checks;
        }
        public void setChecks(int checks) {
            this.checks = checks;
        }

        @PermazenField(storageId = 101)
        @NotNull
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField(storageId = 102)
        @Min(0)
        public abstract int getAge();
        public abstract void setAge(int age);

        @PermazenListField(storageId = 103, element = @PermazenField(storageId = 104))
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

    public abstract static class NameThing implements PermazenObject {

        public abstract String getName();
        public abstract void setName(String name);

        @Override
        public String toString() {
            return this.getName();
        }
    }

// This class has only a type-level JSR 303 constraint

    @PermazenType
    @NonEmptyToString
    public abstract static class NameThing1 extends NameThing {
    }

// This class has only a @OnValidate method

    @PermazenType
    public abstract static class NameThing2 extends NameThing {
        @OnValidate
        public void validateMe() {
            throw new ValidationException(this, "sorry");
        }
    }

// This class extends a class with only a type-level JSR 303 constraint

    @PermazenType
    public abstract static class NameThing3 extends NameThing1 {
    }

// This class implements an interface with a method having a JSR 303 constraint

    public interface Foobar {
        @NotNull
        Object getFoo();
    }

    @PermazenType
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

    @PermazenType
    public abstract static class NameThing5 extends NameThing implements ValidateMe {
        public void validateMe() {
            throw new ValidationException(this, "sorry");
        }
    }

// This class extends a class with only a @OnValidate method

    @PermazenType
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

// Fixup Validation

    public abstract static class AbstractFixupValidation implements PermazenObject {

        protected void fixupNullName() {
            if (this.getName() == null)
                this.setName("placeholder");
        }

        @NotNull
        public abstract String getName();
        public abstract void setName(String name);
    }

    @PermazenType
    public abstract static class EarlyValidation extends AbstractFixupValidation {

        @OnValidate(early = true)
        @Override
        protected void fixupNullName() {
            super.fixupNullName();
        }
    }

    @PermazenType
    public abstract static class LateValidation extends AbstractFixupValidation {

        @OnValidate
        @Override
        protected void fixupNullName() {
            super.fixupNullName();
        }
    }
}
