
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CounterTest extends TestSupport {

    static boolean createFriend;

    @Test
    public void testCounter() {

        final Permazen jdb = BasicTest.getPermazen(Person.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            Person p1 = tx.create(Person.class);

            final Counter counter = p1.getCounter();

            Assert.assertEquals(counter.get(), 0);

            counter.set(123);

            Assert.assertEquals(counter.get(), 123);

            counter.adjust(1);
            counter.adjust(1);
            counter.adjust(1);

            Assert.assertEquals(counter.get(), 126);

            counter.adjust(-200);

            Assert.assertEquals(counter.get(), -74);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person {

        @JField(storageId = 104)
        public abstract Counter getCounter();

        @JMapField(storageId = 101, key = @JField(storageId = 102), value = @JField(storageId = 103, encoding = "float"))
        public abstract NavigableMap<Person, Float> getRatings();
    }
}

