
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.util.ObjIdMap;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.testng.annotations.Test;

public class CopyToWrongTypeTest extends CoreAPITestSupport {

    @Test
    public void testCopyToWrongType() throws Exception {

        // Setup database

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"long\" encoding=\"urn:fdc:permazen.io:2020:long\" storageId=\"2\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"3\">\n"
          + "    <SimpleField name=\"uuid\" encoding=\"urn:fdc:permazen.io:2020:UUID\" storageId=\"4\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"5\" allowDeleted=\"true\">\n"
          + "      <ObjectTypes>\n"
          + "         <ObjectType name=\"Bar\"/>\n"
          + "      </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema);

        final ObjId foo1 = tx.create("Foo");
        final ObjId foo2 = tx.create("Foo");
        tx.writeSimpleField(foo1, "long", 1234L, false);
        final ObjId bar1 = tx.create("Bar");
        final ObjId bar2 = tx.create("Bar");
        tx.writeSimpleField(bar1, "uuid", UUID.randomUUID(), false);
        tx.writeSimpleField(bar2, "uuid", UUID.randomUUID(), false);

        // Try to copy mapping bar1 -> foo1: incompatible type
        final ObjIdMap<ObjId> remap = new ObjIdMap<>(1);
        remap.put(bar1, foo1);
        try {
            tx.copy(bar1, tx, false, false, null, remap);
            assert false : "copied foo1 to bar1!";
        } catch (SchemaMismatchException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Try to copy mapping bar1.ref -> foo1: incompatible type in reference field
        tx.writeSimpleField(bar1, "ref", bar2, false);
        remap.clear();
        remap.put(bar2, foo2);
        try {
            tx.copy(bar1, tx, false, false, null, remap);
            assert false : "copied bar1.ref to foo!";
        } catch (IllegalArgumentException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Try to copy mapping bar2 -> wrong storage ID
        remap.clear();
        remap.put(bar2, new ObjId(1));
        try {
            tx.copy(bar2, tx, false, false, null, remap);
            assert false : "copied bar2 to id#1!";
        } catch (SchemaMismatchException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Try to copy mapping bar2 -> null (i.e., new object)
        remap.clear();
        remap.put(bar2, null);
        tx.copy(bar2, tx, false, false, null, remap);
        ObjId bar2a = remap.get(bar2);
        assert bar2a != null;
        assert !bar2a.equals(bar2);
        assert tx.readSimpleField(bar2a, "uuid", false).equals(tx.readSimpleField(bar2, "uuid", false));
    }
}
