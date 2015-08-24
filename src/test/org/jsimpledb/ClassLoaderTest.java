
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.ObjId;
import org.testng.annotations.Test;

public class ClassLoaderTest extends TestSupport {

/*

    This test tickles a bug that was caused a LinkageError with "attempted  duplicate class definition"
    in our ClassLoader's findClass() method.

    The bug is tickled by loading the snapshot class, which triggers an implicit loading of the normal
    class (which is its superclass), then attempting to load the normal class explicitly.

    The bug was caused by our ClassLoader accepting and defining classes with names having slashes
    instead of dots.

*/
    @Test
    public void testClassLoaderTest() {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            final SnapshotJTransaction stx = jtx.getSnapshotTransaction();
            final ObjId id = new ObjId("0100000000000000");
            stx.getJObject(id).getObjId();                                  // causes snapshot class to load
            jtx.getJObject(id).getObjId();                                  // causes normal class to load redundantly
            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 1)
    public abstract static class Person implements JObject {
    }
}

