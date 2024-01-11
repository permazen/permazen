
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.schema.SchemaId;
import io.permazen.test.TestSupport;
import io.permazen.util.NavigableSets;

import java.util.NavigableSet;

import org.testng.annotations.Test;

public class VersionIntersectTest extends MainTestSupport {

    // Test intersecting the NavigableSets returned by getAll() and querySchemaIndex()
    @SuppressWarnings("unchecked")
    @Test
    public void testVersionIntersect() throws Exception {

        final Permazen pdb = BasicTest.newPermazen(Foo.class, Bar.class);
        final SchemaId schemaId = pdb.getSchemaModel().getSchemaId();

        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            final Foo[] foos = new Foo[4];
            final Bar[] bars = new Bar[4];
            for (int i = 0; i < 4; i++) {
                foos[i] = ptx.create(Foo.class);
                bars[i] = ptx.create(Bar.class);
            }
            final NavigableSet<PermazenObject> set = NavigableSets.<PermazenObject>intersection(
              ptx.querySchemaIndex(PermazenObject.class).get(schemaId),
              (NavigableSet<PermazenObject>)(Object)ptx.getAll(Foo.class));
            TestSupport.checkSet(set, buildSet(foos[0], foos[1], foos[2], foos[3]));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 10)
    public abstract static class Foo implements PermazenObject {
    }

    @PermazenType(storageId = 11)
    public abstract static class Bar implements PermazenObject {
    }
}
