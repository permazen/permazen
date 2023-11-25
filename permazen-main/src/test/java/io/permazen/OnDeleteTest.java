
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.OnDelete;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.test.TestSupport;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnDeleteTest extends TestSupport {

    private static final ThreadLocal<HashSet<JObject>> CALLBACKS = new ThreadLocal<HashSet<JObject>>() {
        @Override
        protected HashSet<JObject> initialValue() {
            return new HashSet<>();
        }
    };

    private static final ThreadLocal<HashMap<JObject, JObject>> DELETE_TARGET = new ThreadLocal<HashMap<JObject, JObject>>() {
        @Override
        protected HashMap<JObject, JObject> initialValue() {
            return new HashMap<>();
        }
    };

    @Test
    public void testOnDelete() {

        final Permazen jdb = BasicTest.getPermazen(Person.class);
        final JTransaction tx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
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
            JTransaction.setCurrent(null);
        }
    }

    private void verify(JObject... jobjs) {
        Assert.assertEquals(CALLBACKS.get(), new HashSet<JObject>(Arrays.asList(jobjs)));
        CALLBACKS.get().clear();
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements JObject {

        @OnDelete
        private void beingDeleted() {
            OnDeleteTest.CALLBACKS.get().add(this);
            final JObject target = DELETE_TARGET.get().remove(this);
            if (target != null)
                target.delete();
        }

        @JField(storageId = 101, inverseDelete = DeleteAction.DELETE)
        public abstract Person getFriend();
        public abstract void setFriend(Person friend);
    }
}
