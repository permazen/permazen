
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.collect.Sets;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaChangeListenerTest extends CoreAPITestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testSchemaChangeListener() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"101\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema1.lockDown(true);
        final SchemaId schemaId1 = schema1.getSchemaId();

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"101\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"102\"/>\n"
          + "    <SetField name=\"set\" storageId=\"103\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"104\"/>\n"
          + "    </SetField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema2.lockDown(true);
        final SchemaId schemaId2 = schema2.getSchemaId();

        final SchemaModel schema3 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"102\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema3.lockDown(true);
        final SchemaId schemaId3 = schema3.getSchemaId();

        final Database db = new Database(kvstore);

    // Tx #1

        Transaction tx = db.createTransaction(schema1);

        ObjId id1 = tx.create("Foo");
        tx.writeSimpleField(id1, "i", 100, true);
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId1);

        tx.commit();

    // Tx #2

        tx = db.createTransaction(schema2);

        final boolean[] notified = new boolean[1];
        tx.addSchemaChangeListener((tx12, id, oldSchemaId, newSchemaId, oldFieldValues) -> {
            Assert.assertEquals(oldSchemaId, schemaId1);
            Assert.assertEquals(newSchemaId, schemaId2);
            Assert.assertEquals(oldFieldValues.keySet(), Sets.newHashSet("i"));
            Assert.assertEquals(oldFieldValues.get("i"), 100);
            notified[0] = true;
        });

        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId1);
        Assert.assertFalse(notified[0]);

        Assert.assertEquals(tx.readSimpleField(id1, "i", false), 100);
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId1);
        Assert.assertFalse(notified[0]);

        Assert.assertEquals(tx.readSimpleField(id1, "i", true), 100);
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId2);
        Assert.assertTrue(notified[0]);

        Assert.assertFalse(tx.migrateSchema(id1));
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId2);
        Assert.assertTrue(notified[0]);

        tx.writeSimpleField(id1, "s", "foobar", true);

        NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id1, "set", true);
        Assert.assertTrue(set.isEmpty());

        set.add(123);
        set.add(456);
        set.add(789);

        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));

        tx.commit();

    // Tx #3

        tx = db.createTransaction(schema3);

        notified[0] = false;
        tx.addSchemaChangeListener((tx1, id, oldSchemaId, newSchemaId, oldFieldValues) -> {
            Assert.assertEquals(oldSchemaId, schemaId2);
            Assert.assertEquals(newSchemaId, schemaId3);
            Assert.assertEquals(oldFieldValues.keySet(), Sets.newHashSet("i", "s", "set"));
            Assert.assertEquals(oldFieldValues.get("i"), 100);
            Assert.assertEquals(oldFieldValues.get("s"), "foobar");
            Assert.assertEquals(oldFieldValues.get("set"), Sets.<Integer>newHashSet(123, 456, 789));
            notified[0] = true;
        });

        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId2);
        Assert.assertFalse(notified[0]);

        set = (NavigableSet<Integer>)tx.readSetField(id1, "set", false);
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId2);
        Assert.assertFalse(notified[0]);

        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));

        Assert.assertTrue(tx.migrateSchema(id1));
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId3);
        Assert.assertTrue(notified[0]);

        Assert.assertEquals(set, new HashSet<Integer>());

        Assert.assertFalse(tx.migrateSchema(id1));

        Assert.assertTrue(notified[0]);
        Assert.assertEquals(tx.getObjType(id1).getSchema().getSchemaId(), schemaId3);

        tx.commit();

    }
}
