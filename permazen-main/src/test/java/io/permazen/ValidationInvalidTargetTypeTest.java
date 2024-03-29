
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;

import org.testng.annotations.Test;

public class ValidationInvalidTargetTypeTest extends MainTestSupport {

    @Test
    public void testValidationInvalidTargetType() {

        final Permazen pdb = BasicTest.newPermazen(Person1.class, Person2.class);

        PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            Person1 p1 = tx.create(Person1.class);
            Person1 p1m = tx.create(Person1.class);

            p1.setFriend(p1m);

            // A bug was causing the following exception on the following statement:
            // java.lang.IllegalArgumentException: invalid target type io.permazen.SelfReferenceTest$HasFriend
            //      for index query on field `friend' in interface io.permazen.SelfReferenceTest$HasFriend:
            //      should be a super-type or sub-type of io.permazen.SelfReferenceTest$Person
            tx.validate();

            tx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    public interface HasFriend {

        @PermazenField(storageId = 200, unique = true)
        HasFriend getFriend();
        void setFriend(HasFriend person);
    }

    public abstract static class Person implements PermazenObject {
    }

    @PermazenType(storageId = 100)
    public abstract static class Person1 extends Person implements HasFriend {
    }

    @PermazenType(storageId = 101)
    public abstract static class Person2 extends Person implements HasFriend {
    }
}
