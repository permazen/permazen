
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SnapshotTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testSnapshot() throws Exception {

        // Setup databases
        final TreeMap<byte[], byte[]> data1 = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
        final NavigableMapKVStore kvstore1 = new NavigableMapKVStore(data1);
        final SimpleKVDatabase kv1 = new SimpleKVDatabase(kvstore1, 100, 500);
        final Database db1 = new Database(kv1);
        final TreeMap<byte[], byte[]> data2 = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
        final NavigableMapKVStore kvstore2 = new NavigableMapKVStore(data2);
        final SimpleKVDatabase kv2 = new SimpleKVDatabase(kvstore2, 100, 500);
        final Database db2 = new Database(kv2);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <Object name=\"Foo\" storageId=\"1\">\n"
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
          + "  </Object>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

    // Setup tx1

        Transaction tx1 = db1.createTransaction(schema, 1, true);

        final ObjId id1 = tx1.create(1);
        final ObjId id2 = tx1.create(1);
        final ObjId id3 = tx1.create(1);

        tx1.writeSimpleField(id1, 2, 123.45f);
        tx1.writeSimpleField(id2, 2, Float.NEGATIVE_INFINITY);
        tx1.writeSimpleField(id3, 2, Float.NaN);

        tx1.writeSimpleField(id1, 3, id2);
        tx1.writeSimpleField(id2, 3, id3);
        tx1.writeSimpleField(id3, 3, id1);

        NavigableSet<Integer> set1 = (NavigableSet<Integer>)tx1.readSetField(id1, 4);
        set1.add(123);
        set1.add(456);

        List<Integer> list1 = (List<Integer>)tx1.readListField(id1, 6);
        list1.add(234);
        list1.add(567);
        list1.add(234);

        NavigableMap<Integer, String> map1 = (NavigableMap<Integer, String>)tx1.readMapField(id1, 8);
        map1.put(987, "foo");
        map1.put(654, "bar");
        map1.put(321, "foo");

    // Setup tx2

        Transaction tx2 = db2.createTransaction(schema, 1, true);

    // Snapshot

        // Copy id1
        Assert.assertTrue(tx1.snapshot(id1, tx2));

        // Verify copy
        Assert.assertTrue(tx2.exists(id1));
        Assert.assertFalse(tx2.exists(id2));
        Assert.assertFalse(tx2.exists(id3));

        // Check fields
        Assert.assertEquals(tx2.getAll(1), buildSet(id1));
        Assert.assertEquals(tx2.readSimpleField(id1, 2), 123.45f);
        Assert.assertEquals(tx2.readSimpleField(id1, 3), id2);
        Assert.assertEquals(tx2.readSetField(id1, 4), buildSet(123, 456));
        Assert.assertEquals(tx2.readListField(id1, 6), buildList(234, 567, 234));
        Assert.assertEquals(tx2.readMapField(id1, 8), buildSortedMap(321, "foo", 654, "bar", 987, "foo"));

        // Check indexes
        Assert.assertEquals(tx2.querySimpleField(3), buildMap(id2, buildSet(id1)));
        Assert.assertEquals(tx2.queryListField(6), buildMap(234, buildSet(id1), 567, buildSet(id1)));
        Assert.assertEquals(tx2.queryMapFieldValue(8), buildMap("foo", buildSet(id1), "bar", buildSet(id1)));

        // Copy id2 and id3
        Assert.assertTrue(tx1.snapshot(id2, tx2));
        Assert.assertTrue(tx1.snapshot(id3, tx2));

        // Verify non-copy of id1 - already copied
        Assert.assertFalse(tx1.snapshot(id1, tx2));

        // Check fields
        Assert.assertEquals(tx2.getAll(1), buildSet(id1, id2, id3));
        Assert.assertEquals(tx2.readSimpleField(id2, 2), Float.NEGATIVE_INFINITY);
        Assert.assertTrue(Float.isNaN((Float)tx2.readSimpleField(id3, 2)));

        Assert.assertEquals(tx2.readSimpleField(id1, 3), id2);
        Assert.assertEquals(tx2.readSimpleField(id2, 3), id3);
        Assert.assertEquals(tx2.readSimpleField(id3, 3), id1);

        // Check indexes
        Assert.assertEquals(tx2.querySimpleField(3), buildMap(id1, buildSet(id3), id2, buildSet(id1), id3, buildSet(id2)));

        // Commit transactions and verify identical key/value stores
        tx1.commit();
        tx2.commit();

        Assert.assertEquals(data1, data2);
    }

    @Test
    public void testSnapshotConflict() throws Exception {

        // Setup databases
        final Database db1 = new Database(new SimpleKVDatabase());
        final Database db2 = new Database(new SimpleKVDatabase());

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <Object name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "  </Object>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <Object name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"false\"/>\n"
          + "  </Object>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx1 = db1.createTransaction(schema1, 1, true);
        Transaction tx2 = db2.createTransaction(schema2, 1, true);

        final ObjId id1 = tx1.create(1);

        try {
            tx1.snapshot(id1, tx2);
            assert false;
        } catch (SchemaMismatchException e) {
            // expected
        }

        tx1.delete(id1);

        try {
            tx1.snapshot(id1, tx2);
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
          + "<Schema>\n"
          + "  <Object name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" type=\"int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "  </Object>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx1 = db1.createTransaction(schema1, 1, true);

        final ObjId id1 = tx1.create(1);

        Assert.assertFalse(tx1.snapshot(id1, tx1));
    }
}

