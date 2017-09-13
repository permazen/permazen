
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;
import io.permazen.util.NavigableSets;

import java.util.NavigableSet;

import org.testng.annotations.Test;

public class VersionIntersectTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testVersionIntersect() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Foo.class, Bar.class);

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            final Foo[] foos = new Foo[4];
            final Bar[] bars = new Bar[4];
            for (int i = 0; i < 4; i++) {
                foos[i] = jtx.create(Foo.class);
                bars[i] = jtx.create(Bar.class);
            }
            final int version = jdb.getConfiguredVersion();
            final NavigableSet<JObject> set = NavigableSets.<JObject>intersection(
              jtx.queryVersion(JObject.class).get(version), (NavigableSet<JObject>)(Object)jtx.getAll(Foo.class));
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

