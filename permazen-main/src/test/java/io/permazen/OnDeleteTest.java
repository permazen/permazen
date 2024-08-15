
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnDelete;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnDeleteTest extends MainTestSupport {

    private static final ThreadLocal<HashSet<PermazenObject>> CALLBACKS = ThreadLocal.withInitial(HashSet::new);

    private static final ThreadLocal<HashMap<PermazenObject, PermazenObject>> DELETE_TARGET = ThreadLocal.withInitial(HashMap::new);

    @Test
    public void testOnDelete() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            Person p1 = tx.create(Person.class);
            Person p2 = tx.create(Person.class);
            Person p3 = tx.create(Person.class);

            this.verify();

            // Verify delete callback
            boolean result = p1.delete();
            Assert.assertTrue(result);
            this.verify(p1);

            // Verify no duplicate delete callback
            result = p1.delete();
            Assert.assertFalse(result);
            this.verify();

            // Verify delete cycle works
            p1 = tx.create(Person.class);
            p1.setFriend(p2);
            p2.setFriend(p3);
            p3.setFriend(p1);
            result = p1.delete();
            Assert.assertTrue(result);
            this.verify(p1, p2, p3);
            result = p2.delete();
            Assert.assertFalse(result);
            result = p3.delete();
            Assert.assertFalse(result);
            this.verify();

            // Check deletion from within deletion callback
            p1 = tx.create(Person.class);
            p2 = tx.create(Person.class);
            p3 = tx.create(Person.class);

            DELETE_TARGET.get().put(p1, p2);
            DELETE_TARGET.get().put(p2, p3);
            DELETE_TARGET.get().put(p3, p1);
            p1.delete();
            this.verify(p1, p2, p3);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private void verify(PermazenObject... jobjs) {
        Assert.assertEquals(CALLBACKS.get(), new HashSet<PermazenObject>(Arrays.asList(jobjs)));
        CALLBACKS.get().clear();
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements PermazenObject {

        @OnDelete
        private void beingDeleted() {
            OnDeleteTest.CALLBACKS.get().add(this);
            final PermazenObject target = DELETE_TARGET.get().remove(this);
            if (target != null)
                target.delete();
        }

        @PermazenField(storageId = 101, inverseDelete = DeleteAction.DELETE)
        public abstract Person getFriend();
        public abstract void setFriend(Person friend);
    }
}
