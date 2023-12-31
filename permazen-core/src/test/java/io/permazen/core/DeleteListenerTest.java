
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteListenerTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteListener() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema);

        final ObjId id1 = tx.create("Foo");
        final ObjId id2 = tx.create("Foo");
        final ObjId id3 = tx.create("Foo");

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
