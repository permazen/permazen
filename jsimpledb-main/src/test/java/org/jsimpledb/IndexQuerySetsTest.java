
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.NavigableSet;
import java.util.UUID;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.NavigableSets;
import org.testng.annotations.Test;

public class IndexQuerySetsTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testNavigableSets() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Foo.class);

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final Foo f1 = jtx.create(Foo.class);
            f1.setUUID(UUID.randomUUID());

            final NavigableSet<Foo> foos = jtx.queryIndex(Foo.class, "UUID", UUID.class).asMap().get(f1.getUUID());

            NavigableSets.<Foo>union(foos, NavigableSets.<Foo>empty());
            NavigableSets.<Foo>union(NavigableSets.<Foo>empty(), foos);

            NavigableSets.<Foo>intersection(foos, NavigableSets.<Foo>empty());
            NavigableSets.<Foo>intersection(NavigableSets.<Foo>empty(), foos);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Foo implements JObject {

        @JField(indexed = true)
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);
    }
}
