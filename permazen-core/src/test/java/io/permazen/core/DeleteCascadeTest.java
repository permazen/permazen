
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

public class DeleteCascadeTest extends CoreAPITestSupport {

    @Test
    public void testDeleteCascade() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Person\" storageId=\"1\">\n"
          + "    <ReferenceField storageId=\"2\" name=\"Person\" cascadeDelete=\"true\" onDelete=\"EXCEPTION\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema, 1, true);

        final ObjId id1 = tx.create(1);
        final ObjId id2 = tx.create(1);

        tx.writeSimpleField(id1, 2, id2, false);

        tx.delete(id1);

        Assert.assertFalse(tx.exists(id1));
        Assert.assertFalse(tx.exists(id2));

        tx.commit();
    }
}
