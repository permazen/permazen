
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.collect.Lists;

import io.permazen.core.util.ObjIdMap;
import io.permazen.kv.KVPair;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DetachedTransactionTest extends CoreAPITestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testDetachedTransaction() throws Exception {

        // Setup database
        final MemoryKVStore kvstore1 = new MemoryKVStore();
        final SimpleKVDatabase kv1 = new SimpleKVDatabase(kvstore1, 100, 500);
        final Database db1 = new Database(kv1);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"float\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"2\"/>\n"
          + "    <ReferenceField name=\"rref\" storageId=\"3\"/>\n"
          + "    <SetField name=\"set\" storageId=\"4\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"5\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"6\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"8\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"9\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"10\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

    // Setup tx1

        Transaction tx1 = db1.createTransaction(schema);

        final ObjId id1 = tx1.create("Foo");
        final ObjId id2 = tx1.create("Foo");
        final ObjId id3 = tx1.create("Foo");

        tx1.writeSimpleField(id1, "float", 123.45f, true);
        tx1.writeSimpleField(id2, "float", Float.NEGATIVE_INFINITY, true);
        tx1.writeSimpleField(id3, "float", Float.NaN, true);

        tx1.writeSimpleField(id1, "rref", id2, true);
        tx1.writeSimpleField(id2, "rref", id3, true);
        tx1.writeSimpleField(id3, "rref", id1, true);

        NavigableSet<Integer> set1 = (NavigableSet<Integer>)tx1.readSetField(id1, "set", true);
        set1.add(123);
        set1.add(456);

        List<Integer> list1 = (List<Integer>)tx1.readListField(id1, "list", true);
        list1.add(234);
        list1.add(567);
        list1.add(234);

        NavigableMap<Integer, String> map1 = (NavigableMap<Integer, String>)tx1.readMapField(id1, "map", true);
        map1.put(987, "foo");
        map1.put(654, "bar");
        map1.put(321, "foo");

    // Setup tx2

        Transaction tx2 = tx1.createDetachedTransaction();

    // Detached

        // Copy id1
        Assert.assertTrue(tx1.copy(id1, tx2, false, false, null, null));

        // Verify copy
        Assert.assertTrue(tx2.exists(id1));
        Assert.assertFalse(tx2.exists(id2));
        Assert.assertFalse(tx2.exists(id3));

        // Check fields
        TestSupport.checkSet(tx2.getAll("Foo"), buildSet(id1));
        Assert.assertEquals(tx2.readSimpleField(id1, "float", true), 123.45f);
        Assert.assertEquals(tx2.readSimpleField(id1, "rref", true), id2);
        TestSupport.checkSet(tx2.readSetField(id1, "set", true), buildSet(123, 456));
        Assert.assertEquals(tx2.readListField(id1, "list", true), buildList(234, 567, 234));
        TestSupport.checkMap(tx2.readMapField(id1, "map", true), buildSortedMap(321, "foo", 654, "bar", 987, "foo"));

        // Check indexes
        TestSupport.checkMap(tx2.querySimpleIndex(3).asMap(), buildMap(id2, buildSet(id1)));
        TestSupport.checkMap(tx2.querySimpleIndex(7).asMap(), buildMap(234, buildSet(id1), 567, buildSet(id1)));
        TestSupport.checkMap(tx2.querySimpleIndex(10).asMap(), buildMap("foo", buildSet(id1), "bar", buildSet(id1)));

        // Copy id2 and id3
        Assert.assertTrue(tx1.copy(id2, tx2, false, false, null, null));
        Assert.assertTrue(tx1.copy(id3, tx2, false, false, null, null));

        // Verify non-copy of id1 - already copied
        Assert.assertFalse(tx1.copy(id1, tx2, false, false, null, null));

        // Check fields
        TestSupport.checkSet(tx2.getAll("Foo"), buildSet(id1, id2, id3));
        Assert.assertEquals(tx2.readSimpleField(id2, "float", true), Float.NEGATIVE_INFINITY);
        Assert.assertTrue(Float.isNaN((Float)tx2.readSimpleField(id3, "float", true)));

        Assert.assertEquals(tx2.readSimpleField(id1, "rref", true), id2);
        Assert.assertEquals(tx2.readSimpleField(id2, "rref", true), id3);
        Assert.assertEquals(tx2.readSimpleField(id3, "rref", true), id1);

        // Check indexes
        TestSupport.checkMap(tx2.querySimpleIndex(3).asMap(),
          buildMap(id1, buildSet(id3), id2, buildSet(id1), id3, buildSet(id2)));

        // Change id1 and then overwrite copy
        tx1.writeSimpleField(id1, "float", 456.78f, true);
        tx1.readSetField(id1, "set", true).clear();
        Assert.assertFalse(tx1.copy(id1, tx2, false, false, null, null));
        Assert.assertEquals(tx2.readSimpleField(id1, "float", true), 456.78f);
        Assert.assertTrue(tx2.readSetField(id1, "set", true).isEmpty());

        // Commit transaction and verify identical key/value stores
        tx1.commit();

        Transaction tx3 = db1.createTransaction(schema);

        Assert.assertEquals(
          Lists.<KVPair>newArrayList(tx3.getKVTransaction().getRange(null, null)),
          Lists.<KVPair>newArrayList(kvstore1.getRange(null, null)));
    }

    @Test
    public void testDetachedConflict() throws Exception {

        // Setup databases
        final Database db1 = new Database(new MemoryKVDatabase());
        final Database db2 = new Database(new MemoryKVDatabase());

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"2\">\n"
          + "    <SimpleField name=\"bar\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"7\" indexed=\"false\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        Transaction tx1 = db1.createTransaction(schema1);
        Transaction tx2 = db2.createTransaction(schema2);

        final ObjId id1 = tx1.create("Foo");

        try {
            tx1.copy(id1, tx2, false, false, null, null);
            assert false;
        } catch (SchemaMismatchException e) {   //
            this.log.debug("got expected {}", e.toString());
        }

        tx1.delete(id1);

        final ObjIdMap<ObjId> remap = new ObjIdMap<>(1);
        remap.put(id1, new ObjId(2));
        try {
            tx1.copy(id1, tx2, false, false, null, remap);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.debug("got expected {}", e.toString());
        }
    }

    @Test
    public void testDetachedSame() throws Exception {

        // Setup databases
        final Database db1 = new Database(new MemoryKVDatabase());

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"bar\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"7\" indexed=\"true\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"8\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        Transaction tx1 = db1.createTransaction(schema1);

        final ObjId id1 = tx1.create("Foo");
        Assert.assertFalse(tx1.copy(id1, tx1, false, false, null, null));

        tx1.writeSimpleField(id1, "bar", 1234, false);
        tx1.writeSimpleField(id1, "ref", id1, false);
        final ObjId id2 = new ObjId(1);
        final ObjIdMap<ObjId> remap = new ObjIdMap<>(1);
        remap.put(id1, id2);
        Assert.assertTrue(tx1.copy(id1, tx1, false, false, null, remap));
        Assert.assertEquals(tx1.readSimpleField(id2, "bar", false), 1234);
        Assert.assertEquals(tx1.readSimpleField(id2, "ref", false), id2);
    }
}
