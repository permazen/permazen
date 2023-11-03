
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.test.TestSupport;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AllowDeletedTest extends TestSupport {

    @Test
    public void testAllowDeleted() {

        final Permazen jdb = BasicTest.getPermazen(Person.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            Person person = jtx.create(Person.class);

            Person deletedPerson = jtx.create(Person.class);
            deletedPerson.delete();

            try {
                person.setDefinitelyExistsFriend(deletedPerson);
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
                // expected
            }

            person.setPossiblyDeletedFriend(deletedPerson);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopy() {

        final Permazen jdb = BasicTest.getPermazen(Person.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final JTransaction stx = jtx.getSnapshotTransaction();

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
            jtx.getAll(Person.class).forEach(JObject::delete);
            try {
                p1.copyIn("definitelyExistsFriend");
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
            }

            // copyIn() of all three objects through reference paths from first object
            jtx.getAll(Person.class).forEach(JObject::delete);
            p1.copyIn("definitelyExistsFriend", "definitelyExistsFriend.definitelyExistsFriend");
            Assert.assertEquals(jtx.getAll(Person.class).size(), 3);

            // copyTo() of 2/3 objects
            jtx.getAll(Person.class).forEach(JObject::delete);
            try {
                stx.copyTo(jtx, new CopyState(), Arrays.asList(p1, p2).stream());
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected {}", e.toString());
                Assert.assertEquals(e.getId(), p3.getObjId());
            }

            // copyTo() of all 3/3 objects
            jtx.getAll(Person.class).forEach(JObject::delete);
            stx.copyTo(jtx, new CopyState(), Arrays.asList(p1, p2, p3).stream());
            Assert.assertEquals(jtx.getAll(Person.class).size(), 3);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements JObject {

        @JField(inverseDelete = DeleteAction.UNREFERENCE)
        public abstract Person getDefinitelyExistsFriend();
        public abstract void setDefinitelyExistsFriend(Person friend);

        @JField(allowDeleted = true)
        public abstract Person getPossiblyDeletedFriend();
        public abstract void setPossiblyDeletedFriend(Person friend);

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }
}
