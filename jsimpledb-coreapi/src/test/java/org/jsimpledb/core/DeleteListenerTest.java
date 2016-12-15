
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;

import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteListenerTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteListener() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes("UTF-8")));

        final Transaction tx = db.createTransaction(schema, 1, true);

        final ObjId id1 = tx.create(1);
        final ObjId id2 = tx.create(1);
        final ObjId id3 = tx.create(1);

        final int[] notify1 = new int[1];
        final int[] notify2 = new int[1];
        final int[] notify3 = new int[1];

        final DeleteListener listener = (tx2, id) -> {
            Assert.assertEquals(tx2, tx);
            if (id.equals(id1)) {
                notify1[0]++;
                tx.delete(id1);
            } else if (id.equals(id2)) {
                notify2[0]++;
                tx.delete(id2);
            } else if (id.equals(id3)) {
                notify3[0]++;
                tx.delete(id2);
            }
        };

        tx.addDeleteListener(listener);
        tx.addDeleteListener(listener);

        tx.delete(id1);

        tx.removeDeleteListener(listener);

        tx.delete(id2);

        tx.addDeleteListener(listener);

        tx.delete(id3);

        Assert.assertEquals(notify1[0], 1);
        Assert.assertEquals(notify2[0], 0);
        Assert.assertEquals(notify3[0], 1);
    }
}

