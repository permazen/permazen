
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.annotations.Test;

public class DroppedTypeTest extends TestSupport {

    @Test
    public void testDroppedType() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" type=\"int\" storageId=\"3\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"2\">\n"
          + "    <SimpleField name=\"val\" type=\"int\" storageId=\"4\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"val\" type=\"int\" storageId=\"3\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

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

