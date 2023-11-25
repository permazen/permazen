
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.util.ObjIdMap;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AllowDeletedTest extends CoreAPITestSupport {

    private SchemaModel schema;

    @BeforeClass
    public void setupDatabase() throws Exception {

        this.schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <ReferenceField name=\"ref1\" storageId=\"10\" allowDeleted=\"false\"/>\n"
          + "    <ReferenceField name=\"ref2\" storageId=\"11\" allowDeleted=\"true\"/>\n"
          + "    <SetField name=\"set1\" storageId=\"20\">\n"
          + "        <ReferenceField storageId=\"22\" allowDeleted=\"false\"/>\n"
          + "    </SetField>"
          + "    <SetField name=\"set2\" storageId=\"21\">\n"
          + "        <ReferenceField storageId=\"23\" allowDeleted=\"true\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list1\" storageId=\"30\">\n"
          + "        <ReferenceField storageId=\"32\" allowDeleted=\"false\"/>\n"
          + "    </ListField>"
          + "    <ListField name=\"list2\" storageId=\"31\">\n"
          + "        <ReferenceField storageId=\"33\" allowDeleted=\"true\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map1\" storageId=\"40\">\n"
          + "        <ReferenceField storageId=\"42\" allowDeleted=\"false\"/>\n"
          + "        <ReferenceField storageId=\"43\" allowDeleted=\"true\"/>\n"
          + "    </MapField>"
          + "    <MapField name=\"map2\" storageId=\"41\">\n"
          + "        <ReferenceField storageId=\"44\" allowDeleted=\"false\"/>\n"
          + "        <ReferenceField storageId=\"45\" allowDeleted=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testAllowDeleted() throws Exception {

        Transaction tx = this.createTx();

        final ObjId id1 = tx.create(1);
        final ObjId id2 = tx.create(1);
        final ObjId id3 = tx.create(1);
        final ObjId id4 = tx.create(1);
        final ObjId deleted1 = tx.create(1);
        final ObjId deleted2 = tx.create(1);
        final ObjId deleted3 = tx.create(1);

        tx.delete(deleted1);
        tx.delete(deleted2);
        tx.delete(deleted3);

        // ref1 - self reference
        tx.writeSimpleField(id1, 10, id1, true);
        tx.writeSimpleField(id1, 10, null, true);

        // ref1 vs. ref2
        try {
            tx.writeSimpleField(id1, 10, deleted1, true);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        tx.writeSimpleField(id1, 11, deleted1, true);

        // set1
        ((Set)tx.readSetField(id1, 20, true)).add(id1);
        ((Set)tx.readSetField(id1, 20, true)).add(id2);
        try {
            ((Set)tx.readSetField(id1, 20, true)).add(deleted1);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        ((Set)tx.readSetField(id1, 20, true)).add(id3);

        // set2
        ((Set)tx.readSetField(id1, 21, true)).add(id1);
        ((Set)tx.readSetField(id1, 21, true)).add(id2);
        ((Set)tx.readSetField(id1, 21, true)).add(deleted1);
        ((Set)tx.readSetField(id1, 21, true)).add(id3);

        // list1
        ((List)tx.readListField(id1, 30, true)).add(id1);
        ((List)tx.readListField(id1, 30, true)).add(id2);
        try {
            ((List)tx.readListField(id1, 30, true)).add(deleted1);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        Assert.assertEquals(tx.readListField(id1, 30, true).size(), 2);
        ((List)tx.readListField(id1, 30, true)).add(id3);
        Assert.assertEquals(tx.readListField(id1, 30, true).size(), 3);
        Assert.assertEquals(tx.readListField(id1, 30, true).get(2), id3);
        try {
            ((List)tx.readListField(id1, 30, true)).set(2, deleted1);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        Assert.assertEquals(tx.readListField(id1, 30, true).size(), 3);
        Assert.assertEquals(tx.readListField(id1, 30, true).get(2), id3);

        // list2
        ((List)tx.readListField(id1, 31, true)).add(id1);
        ((List)tx.readListField(id1, 31, true)).add(id2);
        ((List)tx.readListField(id1, 31, true)).add(deleted1);
        ((List)tx.readListField(id1, 31, true)).add(id3);
        ((List)tx.readListField(id1, 31, true)).set(0, deleted1);

        // map1
        ((Map)tx.readMapField(id1, 40, true)).put(id1, id2);
        try {
            ((Map)tx.readMapField(id1, 40, true)).put(deleted1, id3);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        ((Map)tx.readMapField(id1, 40, true)).put(id4, deleted2);

        // map2
        ((Map)tx.readMapField(id1, 41, true)).put(id1, id2);
        ((Map)tx.readMapField(id1, 41, true)).put(id3, deleted1);
        try {
            ((Map)tx.readMapField(id1, 41, true)).put(deleted2, id4);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }

        tx.commit();
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testAllowDeletedCopy() throws Exception {

        Transaction tx = this.createTx();
        DetachedTransaction stx = tx.createDetachedTransaction();

        final ObjId id1 = stx.create(1);
        final ObjId deleted1 = stx.create(1);

        stx.delete(deleted1);

        // ref1 - self reference should be OK!
        stx.writeSimpleField(id1, 10, id1, true);
        stx.copy(id1, tx, true, false, null, null);

        // ref1
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        stx.writeSimpleField(id1, 10, deleted1, true);
        try {
            this.log.info("doing copy #1: id={} tx={} stx={}", id1, tx, stx);
            stx.copy(id1, tx, true, false, null, null);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        assert !tx.exists(id1);
        final ObjIdMap<ReferenceField> deletedAssignments = new ObjIdMap<>();
        stx.copy(id1, tx, true, false, deletedAssignments, null);
        assert tx.exists(id1);
        this.log.info("deletedAssignments = {}", deletedAssignments);
        checkMap(deletedAssignments, buildMap(deleted1, tx.getSchema().getObjType(1).getField(10, true)));

        // ref2
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        stx.writeSimpleField(id1, 11, deleted1, true);
        stx.copy(id1, tx, true, false, null, null);

        // list1 - self reference should be OK!
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((List)stx.readListField(id1, 30, true)).add(id1);
        stx.copy(id1, tx, true, false, null, null);

        // list1
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((List)stx.readListField(id1, 30, true)).add(deleted1);
        try {
            stx.copy(id1, tx, true, false, null, null);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        assert !tx.exists(id1);
        deletedAssignments.clear();
        stx.copy(id1, tx, true, false, deletedAssignments, null);
        assert tx.exists(id1);
        checkMap(deletedAssignments, buildMap(deleted1, tx.getSchema().getObjType(1).getField(32, true)));

        // list2
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((List)stx.readListField(id1, 31, true)).add(deleted1);
        stx.copy(id1, tx, true, false, null, null);

        // set1
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((Set)stx.readSetField(id1, 20, true)).add(deleted1);
        try {
            stx.copy(id1, tx, true, false, null, null);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        assert !tx.exists(id1);
        deletedAssignments.clear();
        stx.copy(id1, tx, true, false, deletedAssignments, null);
        assert tx.exists(id1);
        checkMap(deletedAssignments, buildMap(deleted1, tx.getSchema().getObjType(1).getField(22, true)));

        // set2
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((Set)stx.readSetField(id1, 21, true)).add(deleted1);
        stx.copy(id1, tx, true, false, null, null);

        // set2 with deletedAssignments
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((Set)stx.readSetField(id1, 21, true)).add(deleted1);
        deletedAssignments.clear();
        stx.copy(id1, tx, true, false, deletedAssignments, null);
        checkMap(deletedAssignments, buildMap());

        // map1
        stx.delete(id1);
        tx.delete(id1);
        stx.create(id1);
        ((Map)stx.readMapField(id1, 40, true)).put(deleted1, id1);
        try {
            stx.copy(id1, tx, true, false, null, null);
            assert false;
        } catch (DeletedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }
        assert !tx.exists(id1);
        deletedAssignments.clear();
        stx.copy(id1, tx, true, false, deletedAssignments, null);
        assert tx.exists(id1);
        checkMap(deletedAssignments, buildMap(deleted1, tx.getSchema().getObjType(1).getField(42, true)));

    }

    private Transaction createTx() {
        final NavigableMapKVStore kvstore = new NavigableMapKVStore();
        final SimpleKVDatabase kv = new SimpleKVDatabase(kvstore, 100, 500);
        final Database db = new Database(kv);
        return db.createTransaction(this.schema, 1, true);
    }
}
