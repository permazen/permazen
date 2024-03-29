
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

// Verify that we can't access collection fields after the owning object is deleted
public class DeletedAccessTest extends CoreAPITestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testDeletedAccess() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <CounterField name=\"counter\" storageId=\"5\"/>\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "      <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"11\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"20\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"30\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"31\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"32\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema);

    // Create object

        final ObjId id = tx.create("Foo");

        final NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id, "set", false);
        final List<Integer> list = (List<Integer>)tx.readListField(id, "list", false);
        final NavigableMap<Integer, String> map = (NavigableMap<Integer, String>)tx.readMapField(id, "map", false);

    // Populate collections

        set.add(123);
        set.add(456);
        set.add(789);

        list.add(321);
        list.add(654);
        list.add(987);

        map.put(555, "wrongnumber");
        map.put(666, "markofdevil");
        map.put(777, "luckyyou");

        final Iterator<Integer> seti = set.iterator();
        final Iterator<Integer> listi = list.iterator();
        final NavigableSet<Integer> mapKey = map.navigableKeySet();
        final Iterator<Integer> mapKeyi = mapKey.iterator();
        final Collection<String> mapVal = map.values();
        final Iterator<String> mapVali = mapVal.iterator();

    // Delete object

        tx.delete(id);

    // Verify counter is inaccessible

        try {
            tx.readCounterField(id, "counter", false);
            assert false : "can access counter after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            tx.writeCounterField(id, "counter", 123L, false);
            assert false : "can access counter after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            tx.adjustCounterField(id, "counter", 99, false);
            assert false : "can access counter after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

    // Verify read-only methods show empty

        Assert.assertTrue(set.isEmpty());
        Assert.assertEquals(set.size(), 0);
        Assert.assertFalse(set.iterator().hasNext());

        Assert.assertTrue(list.isEmpty());
        Assert.assertEquals(list.size(), 0);
        Assert.assertFalse(list.iterator().hasNext());

        Assert.assertTrue(map.isEmpty());
        Assert.assertEquals(map.size(), 0);

        Assert.assertTrue(map.keySet().isEmpty());
        Assert.assertEquals(map.keySet().size(), 0);
        Assert.assertFalse(map.keySet().iterator().hasNext());

        Assert.assertTrue(map.values().isEmpty());
        Assert.assertEquals(map.values().size(), 0);
        Assert.assertFalse(map.values().iterator().hasNext());

    // Verify mutator methods throw exception

        try {
            set.add(999);
            assert false : "can add to set after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            list.add(999);
            assert false : "can add to list after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            map.put(999, "whoops");
            assert false : "can add to map after object is deleted";
        } catch (DeletedObjectException e) {
            // expected
        }

        tx.commit();
    }
}
