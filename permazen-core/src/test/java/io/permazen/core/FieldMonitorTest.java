
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.KeyRanges;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldMonitorTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testFieldMonitors() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final String schemaXML =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"z\" encoding=\"urn:fdc:permazen.io:2020:boolean\" storageId=\"101\"/>\n"
          + "    <SimpleField name=\"b\" encoding=\"urn:fdc:permazen.io:2020:byte\" storageId=\"102\"/>\n"
          + "    <SimpleField name=\"c\" encoding=\"urn:fdc:permazen.io:2020:char\" storageId=\"103\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:short\" storageId=\"104\"/>\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"105\"/>\n"
          + "    <SimpleField name=\"f\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"106\"/>\n"
          + "    <SimpleField name=\"j\" encoding=\"urn:fdc:permazen.io:2020:long\" storageId=\"107\"/>\n"
          + "    <SimpleField name=\"d\" encoding=\"urn:fdc:permazen.io:2020:double\" storageId=\"108\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"109\"/>\n"
          + "    <SimpleField name=\"str\" encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"110\"/>\n"
          + "    <SetField name=\"set\" storageId=\"120\">\n"
          + "        <ReferenceField storageId=\"121\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"130\">\n"
          + "        <ReferenceField storageId=\"131\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"140\">\n"
          + "        <ReferenceField storageId=\"141\"/>\n"
          + "        <ReferenceField storageId=\"142\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"200\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"105\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"109\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(schemaXML.getBytes(StandardCharsets.UTF_8)));

        Transaction tx = db.createTransaction(schema, 1, true);

        final ObjId id1 = new ObjId("6411111111111111");
        final ObjId id2 = new ObjId("6422222222222222");
        final ObjId id3 = new ObjId("6433333333333333");
        final ObjId id4 = new ObjId("6444444444444444");

        Assert.assertTrue(tx.create(id1));
        Assert.assertTrue(tx.create(id2));
        Assert.assertTrue(tx.create(id3));
        Assert.assertTrue(tx.create(id4));

        final NavigableSet<ObjId> set1 = (NavigableSet<ObjId>)tx.readSetField(id1, 120, true);
        final List<ObjId> list1 = (List<ObjId>)tx.readListField(id1, 130, true);
        final NavigableMap<ObjId, ObjId> map1 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id1, 140, true);

        final NavigableSet<ObjId> set2 = (NavigableSet<ObjId>)tx.readSetField(id2, 120, true);
        final List<ObjId> list2 = (List<ObjId>)tx.readListField(id2, 130, true);
        final NavigableMap<ObjId, ObjId> map2 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id2, 140, true);

        final NavigableSet<ObjId> set3 = (NavigableSet<ObjId>)tx.readSetField(id3, 120, true);
        final List<ObjId> list3 = (List<ObjId>)tx.readListField(id3, 130, true);
        final NavigableMap<ObjId, ObjId> map3 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id3, 140, true);

        final NavigableSet<ObjId> set4 = (NavigableSet<ObjId>)tx.readSetField(id4, 120, true);
        final List<ObjId> list4 = (List<ObjId>)tx.readListField(id4, 130, true);
        final NavigableMap<ObjId, ObjId> map4 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id4, 140, true);

    /*

        id1                      id2                      id3                      id4
         |                        |                        |                        |
         +- ref -> id2            +- ref -> id3            +- ref -> id2            +- ref -> null
         |                        |                        |                        |
         +- set                   +- set                   +- set                   +- set
         |   |                    |   |                    |   |                    |   |
         |   +-- id1              |   +-- id2              |   +-- id3              |   +-- null
         |   |                    |   |                    |                        |
         |   +-- id3              |   +-- id4              +- list                  +- list
         |                        |                        |   |                    |   |
         +- list                  +- list                  |   +-- id3              |   +-- null
         |   |                    |   |                    |   |                    |
         |   +-- id4              |   +-- id1              |   +-- id3              +- map
         |   |                    |   |                    |   |                        |
         |   +-- id3              |   +-- id1              |   +-- id3                  +-- { null, null }
         |   |                    |   |                    |
         |   +-- id4              |   +-- id2              +- map
         |                        |                            |
         +- map                   +- map                       +-- { id3, null }
             |                        |                        |
             +-- { null, id2 }        +-- { id3, null }        +-- { null, id3 }
             |                        |
             +-- { id3, id1 }         +-- { id2, id2 }
             |                        |
             +-- { id1, id2 }         +-- { id4, id3 }
             |
             +-- { id2, id4 }

        ref: 109    set: 121    list: 131   map.key: 141    map.value: 142
    */

        tx.writeSimpleField(id1, 109, id2, true);
        tx.writeSimpleField(id2, 109, id3, true);
        tx.writeSimpleField(id3, 109, id2, true);

        set1.addAll(Arrays.asList(id1, id3));
        set2.addAll(Arrays.asList(id2, id4));
        set3.addAll(Arrays.asList(id3));

        list1.addAll(Arrays.asList(id4, id3, id4));
        list2.addAll(Arrays.asList(id1, id1, id2));
        list3.addAll(Arrays.asList(id3, id3, id3));
        list4.addAll(Arrays.asList((ObjId)null));

        map1.put(null, id2);
        map1.put(id3, id1);
        map1.put(id1, id2);
        map1.put(id2, id4);
        map2.put(id3, null);
        map2.put(id2, id2);
        map2.put(id4, id3);
        map3.put(id3, null);
        map3.put(null, id3);
        map4.put(null, null);

        //this.log.info("map1: {}", map1);
        //this.log.info("map2: {}", map2);
        //this.log.info("map3: {}", map3);
        //this.log.info("map4: {}", map4);

        final TestListener listener = new TestListener(tx);
        listener.verify();

        // Test error cases

        try {   // null listener
            tx.addSimpleFieldChangeListener(101, new int[0], null, null);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {   // null path
            tx.addSimpleFieldChangeListener(101, null, null, listener);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {   // unknown target field
            tx.addSimpleFieldChangeListener(999, new int[0], null, listener);
            // expected - we allow this now
        } catch (UnknownFieldException e) {
            assert false;
        }
        try {   // unknown path element
            tx.addSimpleFieldChangeListener(101, new int[] { 999 }, null, listener);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {   // non-reference simple field path element
            tx.addSimpleFieldChangeListener(101, new int[] { 105 }, null, listener);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {   // complex field path element
            tx.addSimpleFieldChangeListener(101, new int[] { 120 }, null, listener);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {   // sub-field target field
            tx.addSimpleFieldChangeListener(121, new int[0], null, listener);
            // expected - we allow this now
        } catch (UnknownFieldException e) {
            assert false;
        }

    // SimpleField

        // ref -> set -> id4 : { id1, id3 }
        tx.addSimpleFieldChangeListener(105, new int[] { 109, 121 }, null, listener);
        tx.writeSimpleField(id4, 105, 4001, true);
        listener.verify(new Notify("SimpleChange", id4, 105, new int[] { 109, 121 }, Arrays.asList(id1, id3), 0, 4001));
        tx.removeSimpleFieldChangeListener(105, new int[] { 109, 121 }, null, listener);

        tx.writeSimpleField(id4, 105, 4002, true);
        listener.verify();

        tx.writeSimpleField(id4, 106, 4.0f, true);
        listener.verify();

        // id4 : { id4 }
        tx.addSimpleFieldChangeListener(105, new int[0], null, listener);
        tx.writeSimpleField(id4, 105, 4003, true);
        listener.verify(new Notify("SimpleChange", id4, 105, new int[0], Arrays.asList(id4), 4002, 4003));
        tx.removeSimpleFieldChangeListener(105, new int[0], null, listener);

        // ref -> id1 : { }
        tx.addSimpleFieldChangeListener(105, new int[] { 109 }, null, listener);
        tx.writeSimpleField(id1, 105, 1001, true);
        listener.verify();
        tx.removeSimpleFieldChangeListener(105, new int[] { 109 }, null, listener);

        // ref -> set -> list -> map.value -> id4 : { id1, id3 }
        tx.addSimpleFieldChangeListener(105, new int[] { 109, 121, 131, 142 }, null, listener);
        tx.writeSimpleField(id4, 105, 4004, true);
        listener.verify(
          new Notify("SimpleChange", id4, 105, new int[] { 109, 121, 131, 142 }, Arrays.asList(id1, id3), 4003, 4004));

        // ref -> set -> list -> map.value -> id3 : { id1, id2, id3 }
        tx.writeSimpleField(id3, 105, 3001, true);
        listener.verify(
          new Notify("SimpleChange", id3, 105, new int[] { 109, 121, 131, 142 }, Arrays.asList(id1, id2, id3), 0, 3001));
        tx.removeSimpleFieldChangeListener(105, new int[] { 109, 121, 131, 142 }, null, listener);

        // ref -> set -> list -> map.key -> id4 : { id1, id3 }
        tx.addSimpleFieldChangeListener(105, new int[] { 109, 121, 131, 141 }, null, listener);
        tx.writeSimpleField(id4, 105, 4005, true);
        listener.verify(
          new Notify("SimpleChange", id4, 105, new int[] { 109, 121, 131, 141 }, Arrays.asList(id1, id3), 4004, 4005));

        // ref -> set -> list -> map.key -> id3 : { id1, id2, id3 }
        tx.writeSimpleField(id3, 105, 3002, true);
        listener.verify(
          new Notify("SimpleChange", id3, 105, new int[] { 109, 121, 131, 141 }, Arrays.asList(id1, id2, id3), 3001, 3002));
        tx.removeSimpleFieldChangeListener(105, new int[] { 109, 121, 131, 141 }, null, listener);

        // list -> list -> id3 : { id1, id2, id3 }
        tx.addSimpleFieldChangeListener(105, new int[] { 131, 131 }, null, listener);
        tx.writeSimpleField(id3, 105, 3003, true);
        listener.verify(new Notify("SimpleChange", id3, 105, new int[] { 131, 131 }, Arrays.asList(id1, id2, id3), 3002, 3003));
        tx.removeSimpleFieldChangeListener(105, new int[] { 131, 131 }, null, listener);

        // list -> id3 : { id1, id3 }
        tx.addSimpleFieldChangeListener(105, new int[] { 131 }, null, listener);
        tx.writeSimpleField(id3, 105, 3004, true);
        listener.verify(new Notify("SimpleChange", id3, 105, new int[] { 131 }, Arrays.asList(id1, id3), 3003, 3004));
        tx.removeSimpleFieldChangeListener(105, new int[] { 131 }, null, listener);

    // SetField

        // set -> id4
        tx.addSetFieldChangeListener(120, new int[] { 121 }, null, listener);
        set4.add(id4);
        listener.verify(new Notify("SetAdd", id4, 120, new int[] { 121 }, Arrays.asList(id2, id4), id4));
        set4.add(id3);
        listener.verify(new Notify("SetAdd", id4, 120, new int[] { 121 }, Arrays.asList(id2, id4), id3));
        set4.remove(id4);
        listener.verify(new Notify("SetRemove", id4, 120, new int[] { 121 }, Arrays.asList(id2), id4));
        set4.clear();
        listener.verify(new Notify("SetClear", id4, 120, new int[] { 121 }, Arrays.asList(id2)));
        tx.removeSetFieldChangeListener(120, new int[] { 121 }, null, listener);

    // Verify we only see changes to specified types

        final ObjId bar = new ObjId("c800000000000000");

        Assert.assertTrue(tx.create(bar));

        tx.writeSimpleField(id1, 109, bar, true);
        tx.writeSimpleField(bar, 109, id1, true);
        tx.writeSimpleField(id1, 105, 5001, true);
        tx.writeSimpleField(bar, 105, 5001, true);

        final KeyRanges range100 = new KeyRanges(ObjId.getKeyRange(100));
        final KeyRanges range200 = new KeyRanges(ObjId.getKeyRange(200));

        tx.addSimpleFieldChangeListener(105, new int[] { 109 }, new KeyRanges[] { null, range100 }, listener);  // type 100 only
        tx.writeSimpleField(bar, 105, 5002, true);
        listener.verify();
        tx.writeSimpleField(id1, 105, 5003, true);
        listener.verify(new Notify("SimpleChange", id1, 105, new int[] { 109 }, Arrays.asList(bar), 5001, 5003));
        tx.removeSimpleFieldChangeListener(105, new int[] { 109 }, new KeyRanges[] { null, range100 }, listener);

        tx.addSimpleFieldChangeListener(105, new int[] { 109 }, new KeyRanges[] { null, range200 }, listener);  // type 200 only
        tx.writeSimpleField(id1, 105, 5004, true);
        listener.verify();
        tx.writeSimpleField(bar, 105, 5005, true);
        listener.verify(new Notify("SimpleChange", bar, 105, new int[] { 109 }, Arrays.asList(id1), 5002, 5005));
        tx.removeSimpleFieldChangeListener(105, new int[] { 109 }, new KeyRanges[] { null, range200 }, listener);

        tx.commit();
    }

    static class TestListener implements SimpleFieldChangeListener, SetFieldChangeListener,
      ListFieldChangeListener, MapFieldChangeListener {

        private final Transaction tx;
        private final ArrayList<Notify> notifys = new ArrayList<>();

        TestListener(Transaction tx) {
            this.tx = tx;
        }

        public void verify(Notify... expected) {
            Assert.assertEquals(this.notifys, Arrays.asList(expected),
              "\nEXPECTED: " + this.showList(Arrays.asList(expected)) + "\n  ACTUAL: " + this.showList(this.notifys) + "\n");
            this.notifys.clear();
        }

        private String showList(List<Notify> list) {
            return list.toString().replaceAll("]], ", "]],\n           ");
        }

        @Override
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("SimpleChange", id, field.getStorageId(), path, referrers, oldValue, newValue));
        }

        @Override
        public <E> void onSetFieldAdd(Transaction tx, ObjId id, SetField<E> field, int[] path,
          NavigableSet<ObjId> referrers, E value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("SetAdd", id, field.getStorageId(), path, referrers, value));
        }

        @Override
        public <E> void onSetFieldRemove(Transaction tx, ObjId id, SetField<E> field, int[] path,
          NavigableSet<ObjId> referrers, E value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("SetRemove", id, field.getStorageId(), path, referrers, value));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("SetClear", id, field.getStorageId(), path, referrers));
        }

        @Override
        public <E> void onListFieldAdd(Transaction tx, ObjId id, ListField<E> field, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("ListAdd", id, field.getStorageId(), path, referrers, index, value));
        }

        @Override
        public <E> void onListFieldRemove(Transaction tx, ObjId id, ListField<E> field, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("ListRemove", id, field.getStorageId(), path, referrers, index, value));
        }

        @Override
        public <E> void onListFieldReplace(Transaction tx, ObjId id, ListField<E> field, int[] path,
          NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("ListChange", id, field.getStorageId(), path, referrers, index, oldValue, newValue));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("ListClear", id, field.getStorageId(), path, referrers));
        }

        @Override
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id, MapField<K, V> field, int[] path,
          NavigableSet<ObjId> referrers, K key, V value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("MapAdd", id, field.getStorageId(), path, referrers, key, value));
        }

        @Override
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id, MapField<K, V> field, int[] path,
          NavigableSet<ObjId> referrers, K key, V value) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("MapRemove", id, field.getStorageId(), path, referrers, key, value));
        }

        @Override
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id, MapField<K, V> field, int[] path,
          NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("MapChange", id, field.getStorageId(), path, referrers, key, oldValue, newValue));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            Assert.assertEquals(tx, this.tx);
            this.notifys.add(new Notify("MapClear", id, field.getStorageId(), path, referrers));
        }
    }

    @Test
    public void testListenerSet() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final String schemaXML =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"105\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(schemaXML.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx1 = db.createTransaction(schema, 1, true);

        final boolean[] flag = new boolean[1];
        tx1.addSimpleFieldChangeListener(105, new int[0], null, new SimpleFieldChangeListener() {
            @Override
            public <V> void onSimpleFieldChange(Transaction tx, ObjId id, SimpleField<V> field,
              int[] path, NavigableSet<ObjId> referrers, V oldValue, V newValue) {
                flag[0] = true;
            }
        });

        final ObjId id = tx1.create(100);
        tx1.writeSimpleField(id, 105, 1234, false);

        Assert.assertTrue(flag[0]);

        final Transaction.ListenerSet listenerSet = tx1.snapshotListeners();

        tx1.commit();

        final Transaction tx2 = db.createTransaction(schema, 1, true);

        tx2.setListeners(listenerSet);

        flag[0] = false;
        tx2.writeSimpleField(id, 105, 5678, false);

        Assert.assertTrue(flag[0]);

        try {
            tx2.addSimpleFieldChangeListener(105, new int[0], null, new SimpleFieldChangeListener() {
                @Override
                public <V> void onSimpleFieldChange(Transaction tx, ObjId id, SimpleField<V> field,
                  int[] path, NavigableSet<ObjId> referrers, V oldValue, V newValue) {
                    // nothing
                }
            });
            assert false : "should not have been able to add new listener";
        } catch (UnsupportedOperationException e) {
            // expected
        }

        tx2.commit();
    }

// Notify

    static class Notify {

        private final String kind;
        private final ObjId id;
        private final int storageId;
        private final int[] path;
        private final ArrayList<ObjId> referrers;
        private final Object[] args;

        Notify(String kind, ObjId id, int storageId, int[] path, Collection<ObjId> referrers, Object... args) {
            this.kind = kind;
            this.id = id;
            this.storageId = storageId;
            this.path = path;
            this.referrers = new ArrayList<>(referrers);
            this.args = args;
        }

        @Override
        public int hashCode() {
            return this.kind.hashCode()
              ^ this.id.hashCode()
              ^ this.storageId
              ^ Arrays.hashCode(this.path)
              ^ this.referrers.hashCode()
              ^ Arrays.hashCode(this.args);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Notify that = (Notify)obj;
            return this.kind.equals(that.kind)
              && this.id.equals(that.id)
              && this.storageId == that.storageId
              && Arrays.equals(this.path, that.path)
              && this.referrers.equals(that.referrers)
              && Arrays.equals(this.args, that.args);
        }

        @Override
        public String toString() {
            return this.kind + "[id=" + this.id + ", field=" + this.storageId + ", path="
              + Arrays.toString(this.path) + ", referrers=" + this.referrers + ", args=" + Arrays.asList(args) + "]";
        }
    }
}
