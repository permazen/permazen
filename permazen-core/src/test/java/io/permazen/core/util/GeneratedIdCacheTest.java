
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.Database;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

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
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\"/>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        Transaction tx = db.createTransaction(schema1);

        final GeneratedIdCache c = new GeneratedIdCache();

        ObjId id1 = c.getGeneratedId(tx, "Foo", "aaa");

        Assert.assertEquals(id1.getStorageId(), 1);
        Assert.assertFalse(tx.exists(id1));

        ObjId id2 = c.getGeneratedId(tx, "Foo", "aaa");
        ObjId id3 = c.getGeneratedId(tx, "Foo", "bbb");

        Assert.assertEquals(id2, id1);
        Assert.assertNotEquals(id3, id1);
        Assert.assertEquals(id3.getStorageId(), 1);
        Assert.assertFalse(tx.exists(id2));
        Assert.assertFalse(tx.exists(id3));

        ObjId id4 = c.getGeneratedId(tx, "Bar", "aaa");

        Assert.assertNotEquals(id4, id1);
        Assert.assertEquals(id4.getStorageId(), 2);

        ObjId id5 = c.getGeneratedId(tx, "Bar", "aaa");
        ObjId id6 = c.getGeneratedId(tx, "Bar", "bbb");

        Assert.assertEquals(id5, id4);
        Assert.assertNotEquals(id6, id4);
        Assert.assertNotEquals(id6, id3);
        Assert.assertEquals(id5.getStorageId(), 2);
        Assert.assertEquals(id6.getStorageId(), 2);

        try {
            c.getGeneratedId(null, "Foo", "aaa");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            c.getGeneratedId(tx, "Foo", null);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            c.getGeneratedId(tx, "Jam", "aaa");
            assert false;
        } catch (UnknownTypeException e) {
            // expected
        }

        tx.commit();
    }
}
