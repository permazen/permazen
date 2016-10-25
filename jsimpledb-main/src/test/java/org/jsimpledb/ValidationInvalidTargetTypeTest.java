
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.testng.annotations.Test;

public class ValidationInvalidTargetTypeTest extends TestSupport {

    @Test
    public void testValidationInvalidTargetType() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person1.class, Person2.class);

        JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            Person1 p1 = tx.create(Person1.class);
            Person1 p1m = tx.create(Person1.class);

            p1.setFriend(p1m);

            // A bug was causing the following exception on the following statement:
            // java.lang.IllegalArgumentException: invalid target type org.jsimpledb.SelfReferenceTest$HasFriend
            //      for index query on field `friend' in interface org.jsimpledb.SelfReferenceTest$HasFriend:
            //      should be a super-type or sub-type of org.jsimpledb.SelfReferenceTest$Person
            tx.validate();

            tx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public interface HasFriend {

        @JField(storageId = 200, unique = true)
        HasFriend getFriend();
        void setFriend(HasFriend person);
    }

    public abstract static class Person implements JObject {
    }

    @JSimpleClass(storageId = 100)
    public abstract static class Person1 extends Person implements HasFriend {
    }

    @JSimpleClass(storageId = 101)
    public abstract static class Person2 extends Person implements HasFriend {
    }
}

