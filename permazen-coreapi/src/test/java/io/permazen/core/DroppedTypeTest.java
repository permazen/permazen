
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

public class DroppedTypeTest extends CoreAPITestSupport {

    @Test
    public void testDroppedType() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"3\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"4\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"3\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema1, 1, true);
        final ObjId foo = tx.create(1);
        final ObjId bar = tx.create(2);
        tx.writeSimpleField(foo, 3, 123, true);
        tx.writeSimpleField(bar, 4, 567, true);

        tx.commit();

        tx = db.createTransaction(schema2, 2, true);

        tx.readSimpleField(foo, 3, true);

        tx.readSimpleField(bar, 4, false);
        try {
            tx.readSimpleField(bar, 4, true);
            assert false;
        } catch (TypeNotInSchemaVersionException e) {
            // expected
        }

        tx.commit();
    }
}

