
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenType;

import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CounterTest extends MainTestSupport {

    static boolean createFriend;

    @Test
    public void testCounter() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
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
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person {

        @PermazenField(storageId = 104)
        public abstract Counter getCounter();

        @PermazenMapField(storageId = 101,
          key = @PermazenField(storageId = 102),
          value = @PermazenField(storageId = 103,
          encoding = "float"))
        public abstract NavigableMap<Person, Float> getRatings();
    }
}
