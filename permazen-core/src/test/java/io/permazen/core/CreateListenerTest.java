
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

public class CreateListenerTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateListener() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\"/>\n"
          + "  <ObjectType name=\"Jam\" storageId=\"3\"/>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema);

        final int[] foos = new int[1];
        final int[] bars = new int[1];
        final int[] jams = new int[1];

        final CreateListener listener = (tx2, id) -> {
            Assert.assertEquals(tx2, tx);
            if (id.getStorageId() == 1)
                foos[0]++;
            else if (id.getStorageId() == 2)
                bars[0]++;
            else if (id.getStorageId() == 3)
                jams[0]++;
            else
                throw new AssertionError();
        };

        tx.addCreateListener(1, listener);
        tx.addCreateListener(2, listener);

        tx.create("Foo");

        Assert.assertEquals(foos[0], 1);
        Assert.assertEquals(bars[0], 0);
        Assert.assertEquals(jams[0], 0);

        tx.create("Bar");

        Assert.assertEquals(foos[0], 1);
        Assert.assertEquals(bars[0], 1);
        Assert.assertEquals(jams[0], 0);

        tx.create("Jam");

        Assert.assertEquals(foos[0], 1);
        Assert.assertEquals(bars[0], 1);
        Assert.assertEquals(jams[0], 0);

        tx.create("Foo");
        tx.create("Bar");
        tx.create("Jam");

        Assert.assertEquals(foos[0], 2);
        Assert.assertEquals(bars[0], 2);
        Assert.assertEquals(jams[0], 0);
    }
}
