
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LeftoverIndexTest extends CoreAPITestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testLeftoverIndex() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"2\">\n"
          + "      <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"3\" indexed=\"true\"/>\n"
          + "    </SetField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);

    // Version 1

        Transaction tx = db.createTransaction(schema1, 1, true);

        final ObjId id1 = tx.create(1);
        NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id1, 2, true);
        set.add(123);

        TestSupport.checkSet(set, buildSet(123));
        TestSupport.checkMap(tx.queryIndex(3).asMap(), buildMap(123, buildSet(id1)));

        tx.commit();

    // Version 2

        tx = db.createTransaction(schema2, 2, true);

        Assert.assertEquals(tx.getSchemaVersion(id1), 1);
        TestSupport.checkMap(tx.queryIndex(3).asMap(), buildMap(123, buildSet(id1)));

        tx.updateSchemaVersion(id1);

        Assert.assertEquals(tx.getSchemaVersion(id1), 2);
        try {
            tx.readSetField(id1, 2, true);
            assert false : "expected UnknownFieldException";
        } catch (UnknownFieldException e) {
            this.log.info("got expected {}", e.toString());
        }
        TestSupport.checkMap(tx.queryIndex(3).asMap(), buildMap());        // verify index is now empty!

        tx.commit();
    }
}

