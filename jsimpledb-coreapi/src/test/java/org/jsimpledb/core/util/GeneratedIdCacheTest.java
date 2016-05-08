
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.util;

import java.io.ByteArrayInputStream;

import org.jsimpledb.core.CoreAPITestSupport;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GeneratedIdCacheTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testGeneratedIdCache() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\"/>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx = db.createTransaction(schema1, 1, true);

        final GeneratedIdCache c = new GeneratedIdCache();

        ObjId id1 = c.getGeneratedId(tx, 1, "aaa");

        Assert.assertEquals(id1.getStorageId(), 1);
        Assert.assertFalse(tx.exists(id1));

        ObjId id2 = c.getGeneratedId(tx, 1, "aaa");
        ObjId id3 = c.getGeneratedId(tx, 1, "bbb");

        Assert.assertEquals(id2, id1);
        Assert.assertNotEquals(id3, id1);
        Assert.assertEquals(id3.getStorageId(), 1);
        Assert.assertFalse(tx.exists(id2));
        Assert.assertFalse(tx.exists(id3));

        ObjId id4 = c.getGeneratedId(tx, 2, "aaa");

        Assert.assertNotEquals(id4, id1);
        Assert.assertEquals(id4.getStorageId(), 2);

        ObjId id5 = c.getGeneratedId(tx, 2, "aaa");
        ObjId id6 = c.getGeneratedId(tx, 2, "bbb");

        Assert.assertEquals(id5, id4);
        Assert.assertNotEquals(id6, id4);
        Assert.assertNotEquals(id6, id3);
        Assert.assertEquals(id5.getStorageId(), 2);
        Assert.assertEquals(id6.getStorageId(), 2);

        try {
            c.getGeneratedId(null, 1, "aaa");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            c.getGeneratedId(tx, 1, null);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            c.getGeneratedId(tx, 3, "aaa");
            assert false;
        } catch (UnknownTypeException e) {
            // expected
        }

        tx.commit();
    }
}

