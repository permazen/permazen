
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SimpleFieldChange;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnChangeNestedGetterTest extends MainTestSupport {

    @Test
    public void testSimpleFieldChange() {

        final Permazen jdb = BasicTest.newPermazen(Person.class);
        final JTransaction tx = jdb.createTransaction(ValidationMode.AUTOMATIC);
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

    @PermazenType(storageId = 100)
    public abstract static class Person implements JObject {

        @io.permazen.annotation.JField(storageId = 101)
        public abstract UUID getId();
        public abstract void setId(UUID id);

        @OnChange("id")
        private void change(SimpleFieldChange<Person, ?> change) {
            Assert.assertEquals(this.getId(), change.getNewValue());
        }
    }
}
