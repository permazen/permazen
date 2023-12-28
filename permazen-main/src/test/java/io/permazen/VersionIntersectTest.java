
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

        final Permazen jdb = BasicTest.newPermazen(Foo.class, Bar.class);
        final SchemaId schemaId = jdb.getSchemaModel().getSchemaId();

        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final Foo[] foos = new Foo[4];
            final Bar[] bars = new Bar[4];
            for (int i = 0; i < 4; i++) {
                foos[i] = jtx.create(Foo.class);
                bars[i] = jtx.create(Bar.class);
            }
            final NavigableSet<JObject> set = NavigableSets.<JObject>intersection(
              jtx.querySchemaIndex(JObject.class).get(schemaId),
              (NavigableSet<JObject>)(Object)jtx.getAll(Foo.class));
            TestSupport.checkSet(set, buildSet(foos[0], foos[1], foos[2], foos[3]));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 10)
    public abstract static class Foo implements JObject {
    }

    @PermazenType(storageId = 11)
    public abstract static class Bar implements JObject {
    }
}
