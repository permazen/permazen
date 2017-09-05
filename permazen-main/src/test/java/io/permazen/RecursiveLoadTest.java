
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JSimpleClass;
import io.permazen.annotation.OnChange;
import io.permazen.change.SimpleFieldChange;
import io.permazen.test.TestSupport;

import org.testng.annotations.Test;

public class RecursiveLoadTest extends TestSupport {

    @Test
    public void testRecursiveLoad() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            tx.create(Person.class);

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
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

