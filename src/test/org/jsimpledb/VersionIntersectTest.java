
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.NavigableSet;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.util.NavigableSets;
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
            final NavigableSet<JObject> set = NavigableSets.<JObject>intersection(
              jtx.queryVersion(JObject.class).get(1), (NavigableSet<JObject>)(Object)jtx.getAll(Foo.class));
            TestSupport.checkSet(set, buildSet(foos[0], foos[1], foos[2], foos[3]));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 10)
    public abstract static class Foo implements JObject {
    }

    @JSimpleClass(storageId = 11)
    public abstract static class Bar implements JObject {
    }
}

