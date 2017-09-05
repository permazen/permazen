
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UserMetaDataTest extends CoreAPITestSupport {

    @Test
    public void testUserMetaData() throws Exception {

        // Setup database with meta-data
        final NavigableMapKVStore kvstore = new NavigableMapKVStore();
        kvstore.put(Layout.getUserMetaDataKeyPrefix(), new byte[] { 1, 2, 3});

        // Create database
        final SimpleKVDatabase kv = new SimpleKVDatabase(kvstore, 100, 500);
        final Database db = new Database(kv);

        // Create objects
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"float\" type=\"float\" storageId=\"2\"/>\n"
          + "    <ReferenceField name=\"rref\" storageId=\"3\"/>\n"
          + "    <SetField name=\"set\" storageId=\"4\">\n"
          + "        <SimpleField type=\"int\" storageId=\"5\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"6\">\n"
          + "        <SimpleField type=\"int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"8\">\n"
          + "        <SimpleField type=\"int\" storageId=\"9\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"10\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

    // Setup tx

        final Transaction tx = db.createTransaction(schema, 1, true);

        tx.create(1);

        tx.commit();

        // Verify meta-data still there

        final byte[] data = kvstore.get(Layout.getUserMetaDataKeyPrefix());
        Assert.assertEquals(data, new byte[] { 1, 2, 3 });
    }
}
