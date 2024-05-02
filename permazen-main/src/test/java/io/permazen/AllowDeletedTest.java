
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AllowDeletedTest extends MainTestSupport {

    @Test
    public void testAllowDeleted() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            Person person = ptx.create(Person.class);

            Person deletedPerson = ptx.create(Person.class);
            deletedPerson.delete();

            try {
                person.setDefinitelyExistsFriend(deletedPerson);
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
                // expected
            }

            person.setPossiblyDeletedFriend(deletedPerson);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopy() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final PermazenTransaction stx = ptx.getDetachedTransaction();

            Person p1 = stx.create(Person.class);
            Person p2 = stx.create(Person.class);
            Person p3 = stx.create(Person.class);

            p1.setDefinitelyExistsFriend(p2);
            p2.setDefinitelyExistsFriend(p3);
            p3.setDefinitelyExistsFriend(p1);

            // copyIn() of only one object
            try {
                p1.copyIn();
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
            }

            // copyIn() of one object and one other object it refers to
            ptx.getAll(Person.class).forEach(PermazenObject::delete);
            try {
                p1.copyTo(ptx, 1, new CopyState(), "definitelyExistsFriend");
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
            }

            // copyIn() of all three objects through reference paths from first object
            ptx.getAll(Person.class).forEach(PermazenObject::delete);
            p1.copyIn("definitelyExistsFriend");
            Assert.assertEquals(ptx.getAll(Person.class).size(), 3);

            // copyTo() of 2/3 objects
            ptx.getAll(Person.class).forEach(PermazenObject::delete);
            try {
                stx.copyTo(ptx, new CopyState(), Arrays.asList(p1, p2).stream());
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
                Assert.assertEquals(e.getId(), p3.getObjId());
            }

            // copyTo() of all 3/3 objects
            ptx.getAll(Person.class).forEach(PermazenObject::delete);
            stx.copyTo(ptx, new CopyState(), Arrays.asList(p1, p2, p3).stream());
            Assert.assertEquals(ptx.getAll(Person.class).size(), 3);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        @PermazenField(inverseDelete = DeleteAction.NULLIFY, forwardCascades = "definitelyExistsFriend")
        public abstract Person getDefinitelyExistsFriend();
        public abstract void setDefinitelyExistsFriend(Person friend);

        @PermazenField(allowDeleted = true)
        public abstract Person getPossiblyDeletedFriend();
        public abstract void setPossiblyDeletedFriend(Person friend);

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }
}
