
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CounterUpgradeTest extends CoreAPITestSupport {

    @Test
    public void testCounterUpgrade() throws Exception {

        String xml1 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\"/>\n"
          + "</Schema>";

        String xml2 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>";

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream(xml1.getBytes(StandardCharsets.UTF_8)));
        this.log.debug("CREATING TX1 WITH \"{}\"", schema1.getSchemaId());
        final Transaction tx1 = db.createTransaction(schema1);
        final ObjId id = tx1.create("Foo");
        tx1.commit();

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream(xml2.getBytes(StandardCharsets.UTF_8)));
        this.log.debug("CREATING TX2 WITH \"{}\"", schema2.getSchemaId());
        final Transaction tx2 = db.createTransaction(schema2);
        tx2.adjustCounterField(id, "counter", 123, true);
        Assert.assertEquals(tx2.readCounterField(id, "counter", true), 123L);
        tx2.commit();
    }
}
