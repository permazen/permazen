
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.annotations.Test;

public class CopyToWrongTypeTest extends TestSupport {

    @Test
    public void testCopyToWrongType() throws Exception {

        // Setup database

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"long\" type=\"long\" storageId=\"2\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"3\">\n"
          + "    <SimpleField name=\"uuid\" type=\"java.util.UUID\" storageId=\"4\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema, 1, true);

        final ObjId foo = tx.create(1);
        tx.writeSimpleField(foo, 2, 1234L, false);
        final ObjId bar = tx.create(3);
        tx.writeSimpleField(bar, 4, UUID.randomUUID(), false);

        try {
            tx.copy(bar, foo, tx, false);
            assert false : "copied foo to bar!";
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}

