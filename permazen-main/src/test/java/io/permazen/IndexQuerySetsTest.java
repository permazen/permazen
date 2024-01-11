
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.util.NavigableSets;

import java.util.NavigableSet;
import java.util.UUID;

import org.testng.annotations.Test;

public class IndexQuerySetsTest extends MainTestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testNavigableSets() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Foo.class);

        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Foo f1 = ptx.create(Foo.class);
            f1.setUUID(UUID.randomUUID());

            final NavigableSet<Foo> foos = ptx.querySimpleIndex(Foo.class, "UUID", UUID.class).asMap().get(f1.getUUID());

            NavigableSets.<Foo>union(foos, NavigableSets.<Foo>empty());
            NavigableSets.<Foo>union(NavigableSets.<Foo>empty(), foos);

            NavigableSets.<Foo>intersection(foos, NavigableSets.<Foo>empty());
            NavigableSets.<Foo>intersection(NavigableSets.<Foo>empty(), foos);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Foo implements PermazenObject {

        @PermazenField(indexed = true)
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);
    }
}
