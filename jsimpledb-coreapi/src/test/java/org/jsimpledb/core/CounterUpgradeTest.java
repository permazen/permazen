
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;

import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CounterUpgradeTest extends CoreAPITestSupport {

    @Test
    public void testCounterUpgrade() throws Exception {

        String xml1 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\"/>\n"
          + "</Schema>";

        String xml2 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>";

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream(xml1.getBytes("UTF-8")));
        final Transaction tx1 = db.createTransaction(schema1, 1, true);
        final ObjId id = tx1.create(10);
        tx1.commit();

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream(xml2.getBytes("UTF-8")));
        final Transaction tx2 = db.createTransaction(schema2, 2, true);
        tx2.adjustCounterField(id, 20, 123, true);
        Assert.assertEquals(tx2.readCounterField(id, 20, true), 123L);
        tx2.commit();
    }
}
