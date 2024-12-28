
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.mvcc.MemoryAtomicKVStore;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteData;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UserMetaDataTest extends CoreAPITestSupport {

    @Test
    public void testUserMetaData() throws Exception {

        // Setup database with meta-data
        final MemoryAtomicKVStore kvstore = new MemoryAtomicKVStore();
        final ByteData metaData = ByteData.of(1, 2, 3);
        kvstore.put(Layout.getUserMetaDataKeyPrefix(), metaData);

        // Create database
        final SimpleKVDatabase kv = new SimpleKVDatabase(kvstore, 100, 500);
        final Database db = new Database(kv);

        // Create objects
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"float\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"2\"/>\n"
          + "    <ReferenceField name=\"rref\" storageId=\"3\"/>\n"
          + "    <SetField name=\"set\" storageId=\"4\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"5\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"6\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"8\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"9\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"10\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

    // Setup tx

        final Transaction tx = db.createTransaction(schema);

        tx.create("Foo");

        tx.commit();

        // Verify meta-data still there

        final ByteData data = kvstore.get(Layout.getUserMetaDataKeyPrefix());
        Assert.assertEquals(data, metaData);
    }
}
