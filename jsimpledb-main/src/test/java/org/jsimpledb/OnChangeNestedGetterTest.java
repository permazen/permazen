
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.UUID;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnChange;
import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OnChangeNestedGetterTest extends TestSupport {

    @Test
    public void testSimpleFieldChange() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person p = tx.create(Person.class);
            p.setId(UUID.randomUUID());
            p.setId(null);
            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements JObject {

        @org.jsimpledb.annotation.JField(storageId = 101)
        public abstract UUID getId();
        public abstract void setId(UUID id);

        @OnChange("id")
        private void change(SimpleFieldChange<Person, ?> change) {
            Assert.assertEquals(this.getId(), change.getNewValue());
        }
    }
}

