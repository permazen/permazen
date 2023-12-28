
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SimpleFieldChange;

import org.testng.annotations.Test;

public class RecursiveLoadTest extends MainTestSupport {

    @Test
    public void testRecursiveLoad() {

        final Permazen jdb = BasicTest.newPermazen(Person.class);
        final JTransaction tx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            tx.create(Person.class);

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements JObject {

        protected Person() {
            this.setName("Some name");
        }

        public abstract String getName();
        public abstract void setName(String name);

        @OnChange("name")
        private void nameChanged(SimpleFieldChange<Person, String> change) {
        }
    }
}
