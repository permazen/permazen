
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import javax.validation.constraints.Min;

import io.permazen.annotation.JField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.kv.RetryTransactionException;
import io.permazen.test.TestSupport;
import org.testng.annotations.Test;

public class ValidationExceptionTest extends TestSupport {

    @Test
    public void testValidationException() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Retryer.class);

        // Transaction with validation disabled
        JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        Retryer jobj;
        try {

            jobj = tx.create(Retryer.class);
            jobj.setReal(123);

            // Attempt validation - we should get the unwrapped exception
            jobj.revalidate();
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
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Retryer implements JObject {

        @JField
        @Min(0)
        public abstract int getReal();
        public abstract void setReal(int real);

        @Min(0)
        public int getDummy() {
            throw new RetryTransactionException(this.getTransaction().getTransaction().getKVTransaction(),
              "simulated retry exception");
        }
        public void setDummy(int dummy) {
        }
    }
}

