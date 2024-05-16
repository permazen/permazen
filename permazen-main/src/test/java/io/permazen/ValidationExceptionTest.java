
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.kv.RetryTransactionException;

import jakarta.validation.constraints.Min;

import org.testng.annotations.Test;

public class ValidationExceptionTest extends MainTestSupport {

    @Test
    public void testValidationException() {

        final Permazen pdb = BasicTest.newPermazen(Retryer.class);

        // Transaction with validation disabled
        PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        Retryer pobj;
        try {

            pobj = tx.create(Retryer.class);
            pobj.setReal(123);

            // Attempt validation - we should get the unwrapped exception
            pobj.revalidate();
            try {
                tx.validate();
                assert false;
            } catch (RetryTransactionException e) {
                // expected
            } catch (ValidationException e) {
                assert false;
            }

            tx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Retryer implements PermazenObject {

        @PermazenField
        @Min(0)
        public abstract int getReal();
        public abstract void setReal(int real);

        @Min(0)
        public int getDummy() {
            throw new RetryTransactionException(this.getPermazenTransaction().getTransaction().getKVTransaction(),
              "simulated retry exception");
        }
        public void setDummy(int dummy) {
        }
    }
}
