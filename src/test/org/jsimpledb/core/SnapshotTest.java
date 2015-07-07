
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SnapshotTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testSnapshot() throws Exception {

        // Setup database
        final NavigableMapKVStore kvstore1 = new NavigableMapKVStore();
        final SimpleKVDatabase kv1 = new SimpleKVDatabase(kvstore1, 100, 500);
        final Database db1 = new Database(kv1);

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

    // Setup tx1

        Transaction tx1 = db1.createTransaction(schema, 1, true);

        final ObjId id1 = tx1.create(1);
        final ObjId id2 = tx1.create(1);
        final ObjId id3 = tx1.create(1);

        tx1.writeSimpleField(id1, 2, 123.45f, true);
        tx1.writeSimpleField(id2, 2, Float.NEGATIVE_INFINITY, true);
        tx1.writeSimpleField(id3, 2, Float.NaN, true);

        tx1.writeSimpleField(id1, 3, id2, true);
        tx1.writeSimpleField(id2, 3, id3, true);
        tx1.writeSimpleField(id3, 3, id1, true);

        NavigableSet<Integer> set1 = (NavigableSet<Integer>)tx1.readSetField(id1, 4, true);
        set1.add(123);
        set1.add(456);

        List<Integer> list1 = (List<Integer>)tx1.readListField(id1, 6, true);
        list1.add(234);
        list1.add(567);
        list1.add(234);

        NavigableMap<Integer, String> map1 = (NavigableMap<Integer, String>)tx1.readMapField(id1, 8, true);
        map1.put(987, "foo");
        map1.put(654, "bar");
        map1.put(321, "foo");

    // Setup tx2

        Transaction tx2 = tx1.createSnapshotTransaction();

    // Snapshot

        // Copy id1
        Assert.assertTrue(tx1.copy(id1, id1, tx2, false));

        // Verify copy
        Assert.assertTrue(tx2.exists(id1));
        Assert.assertFalse(tx2.exists(id2));
        Assert.assertFalse(tx2.exists(id3));

        // Check fields
        TestSupport.checkSet(tx2.getAll(1), buildSet(id1));
        Assert.assertEquals(tx2.readSimpleField(id1, 2, true), 123.45f);
        Assert.assertEquals(tx2.readSimpleField(id1, 3, true), id2);
        TestSupport.checkSet(tx2.readSetField(id1, 4, true), buildSet(123, 456));
        Assert.assertEquals(tx2.readListField(id1, 6, true), buildList(234, 567, 234));
        TestSupport.checkMap(tx2.readMapField(id1, 8, true), buildSortedMap(321, "foo", 654, "bar", 987, "foo"));

        // Check indexes
        TestSupport.checkMap(tx2.queryIndex(3).asMap(), buildMap(id2, buildSet(id1)));
        TestSupport.checkMap(tx2.queryIndex(7).asMap(), buildMap(234, buildSet(id1), 567, buildSet(id1)));
        TestSupport.checkMap(tx2.queryIndex(10).asMap(), buildMap("foo", buildSet(id1), "bar", buildSet(id1)));

        // Copy id2 and id3
        Assert.assertTrue(tx1.copy(id2, id2, tx2, false));
        Assert.assertTrue(tx1.copy(id3, id3, tx2, false));

        // Verify non-copy of id1 - already copied
        Assert.assertFalse(tx1.copy(id1, id1, tx2, false));

        // Check fields
        TestSupport.checkSet(tx2.getAll(1), buildSet(id1, id2, id3));
        Assert.assertEquals(tx2.readSimpleField(id2, 2, true), Float.NEGATIVE_INFINITY);
        Assert.assertTrue(Float.isNaN((Float)tx2.readSimpleField(id3, 2, true)));

        Assert.assertEquals(tx2.readSimpleField(id1, 3, true), id2);
        Assert.assertEquals(tx2.readSimpleField(id2, 3, true), id3);
        Assert.assertEquals(tx2.readSimpleField(id3, 3, true), id1);

        // Check indexes
        TestSupport.checkMap(tx2.queryIndex(3).asMap(),
          buildMap(id1, buildSet(id3), id2, buildSet(id1), id3, buildSet(id2)));

        // Change id1 and then overwrite copy
        tx1.writeSimpleField(id1, 2, 456.78f, true);
        tx1.readSetField(id1, 4, true).clear();
        Assert.assertFalse(tx1.copy(id1, id1, tx2, false));
        Assert.assertEquals(tx2.readSimpleField(id1, 2, true), 456.78f);
        Assert.assertTrue(tx2.readSetField(id1, 4, true).isEmpty());

        // Commit transaction and verify identical key/value stores
        tx1.commit();

        Transaction tx3 = db1.createTransaction(schema, 1, true);

        Assert.assertEquals(
          Lists.<KVPair>newArrayList(tx3.getKVTransaction().getRange(null, null, false)),
          Lists.<KVPair>newArrayList(kvstore1.getRange(null, null, false)));
    }

    @Test
    public void testSnapshotConflict() throws Exception {

        // Setup databases
        final Database db1 = new Database(new SimpleKVDatabase());
        final Database db2 = new Database(new SimpleKVDatabase());

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"false\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx1 = db1.createTransaction(schema1, 1, true);
        Transaction tx2 = db2.createTransaction(schema2, 1, true);

        final ObjId id1 = tx1.create(1);

        try {
            tx1.copy(id1, id1, tx2, false);
            assert false;
        } catch (SchemaMismatchException e) {
            // expected
        }

        tx1.delete(id1);

        try {
            tx1.copy(id1, new ObjId(1), tx2, false);
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
    }

    @Test
    public void testSnapshotSame() throws Exception {

        // Setup databases
        final Database db1 = new Database(new SimpleKVDatabase());

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"8\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx1 = db1.createTransaction(schema1, 1, true);

        final ObjId id1 = tx1.create(1);
        Assert.assertFalse(tx1.copy(id1, id1, tx1, false));

        tx1.writeSimpleField(id1, 7, 1234, false);
        tx1.writeSimpleField(id1, 8, id1, false);
        final ObjId id2 = new ObjId(1);
        Assert.assertTrue(tx1.copy(id1, id2, tx1, false));
        Assert.assertEquals(tx1.readSimpleField(id2, 7, false), 1234);
        Assert.assertEquals(tx1.readSimpleField(id2, 8, false), id1);
    }
}

