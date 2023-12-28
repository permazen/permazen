
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.util.NavigableSets;

import java.util.NavigableSet;
import java.util.UUID;

import org.testng.annotations.Test;

public class IndexQuerySetsTest extends MainTestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testNavigableSets() throws Exception {

        final Permazen jdb = BasicTest.newPermazen(Foo.class);

        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final Foo f1 = jtx.create(Foo.class);
            f1.setUUID(UUID.randomUUID());

            final NavigableSet<Foo> foos = jtx.querySimpleIndex(Foo.class, "UUID", UUID.class).asMap().get(f1.getUUID());

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

    @PermazenType
    public abstract static class Foo implements JObject {

        @JField(indexed = true)
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);
    }
}
