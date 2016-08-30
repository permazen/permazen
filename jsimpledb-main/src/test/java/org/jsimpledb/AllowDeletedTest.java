
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnDelete;
import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AllowDeletedTest extends TestSupport {

    @Test
    public void testAllowDeleted() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            Person person = jtx.create(Person.class);

            Person deletedPerson = jtx.create(Person.class);
            deletedPerson.delete();

            try {
                person.setDefinitelyExistsFriend(deletedPerson);
            } catch (DeletedObjectException e) {
                this.log.debug("got expected " + e);
                // expected
            }

            person.setPossiblyDeletedFriend(deletedPerson);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopy() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
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
                this.log.debug("got expected " + e);
            }

            // copyIn() of one object and one other object it refers to
            for (Person p : jtx.getAll(Person.class))
                p.delete();
            try {
                p1.copyIn("definitelyExistsFriend");
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected " + e);
            }

            // copyIn() of all three objects through reference paths from first object
            for (Person p : jtx.getAll(Person.class))
                p.delete();
            p1.copyIn("definitelyExistsFriend", "definitelyExistsFriend.definitelyExistsFriend");
            Assert.assertEquals(jtx.getAll(Person.class).size(), 3);

            // copyTo() of 2/3 objects
            for (Person p : jtx.getAll(Person.class))
                p.delete();
            try {
                stx.copyTo(jtx, new CopyState(), Arrays.asList(p1, p2));
                assert false;
            } catch (DeletedObjectException e) {
                this.log.debug("got expected " + e);
                Assert.assertEquals(e.getId(), p3.getObjId());
            }

            // copyTo() of all 3/3 objects
            for (Person p : jtx.getAll(Person.class))
                p.delete();
            stx.copyTo(jtx, new CopyState(), Arrays.asList(p1, p2, p3));
            Assert.assertEquals(jtx.getAll(Person.class).size(), 3);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        @JField(onDelete = DeleteAction.UNREFERENCE)
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
