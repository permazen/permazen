
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

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            tx.create(Person.class);

            tx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        @SuppressWarnings("this-escape")
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
