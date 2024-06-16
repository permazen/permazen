
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

public class DroppedTypeTest extends CoreAPITestSupport {

    @Test
    public void testDroppedType() throws Exception {

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(new MemoryKVDatabase());

        Transaction tx = db.createTransaction(schema1);
        final ObjId foo = tx.create("Foo");
        final ObjId bar = tx.create("Bar");
        tx.writeSimpleField(foo, "val", 123, true);
        tx.writeSimpleField(bar, "val", 567, true);

        tx.commit();

        final TransactionConfig config2 = TransactionConfig.builder()
          .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
          .schemaModel(schema2)
          .build();
        tx = db.createTransaction(config2);

        tx.readSimpleField(foo, "val", true);

        final int x = (Integer)tx.readSimpleField(bar, "val", false);
        assert x == 567;

        try {
            tx.readSimpleField(bar, "val", true);
            assert false;
        } catch (TypeNotInSchemaException e) {
            // expected
        }

        tx.commit();
    }
}
