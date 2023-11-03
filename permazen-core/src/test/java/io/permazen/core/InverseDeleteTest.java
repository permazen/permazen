
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple3;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class InverseDeleteTest extends CoreAPITestSupport {

    private static final String XML_TEMPLATE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<Schema>\n"
      + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
      + "    <ReferenceField name=\"ref\" storageId=\"2\" inverseDelete=\"@INVERSE_DELETE@\"/>\n"
      + "    <SimpleField name=\"name\" storageId=\"3\" encoding=\"urn:fdc:permazen.io:2020:String\"/>\n"
      + "    <SetField name=\"set\" storageId=\"10\">\n"
      + "        <ReferenceField storageId=\"20\" inverseDelete=\"@INVERSE_DELETE@\"/>\n"
      + "    </SetField>"
      + "    <ListField name=\"list\" storageId=\"11\">\n"
      + "        <ReferenceField storageId=\"21\" inverseDelete=\"@INVERSE_DELETE@\"/>\n"
      + "    </ListField>"
      + "    <MapField name=\"map1\" storageId=\"12\">\n"
      + "        <ReferenceField storageId=\"22\" inverseDelete=\"@INVERSE_DELETE@\"/>\n"
      + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"23\"/>\n"
      + "    </MapField>"
      + "    <MapField name=\"map2\" storageId=\"13\">\n"
      + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"24\"/>\n"
      + "        <ReferenceField storageId=\"25\" inverseDelete=\"@INVERSE_DELETE@\"/>\n"
      + "    </MapField>"
      + "  </ObjectType>\n"
      + "</Schema>\n";

    @Test
    @SuppressWarnings("unchecked")
    public void testInverseDelete() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        for (DeleteAction inverseDelete : DeleteAction.values()) {
            final String xml = XML_TEMPLATE.replaceAll("@INVERSE_DELETE@", inverseDelete.name());
            final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

            ObjId id1;
            ObjId id2;
            ObjId id3;
            Transaction tx;

        // Referencefield

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            tx.writeSimpleField(id1, 2, id2, true);
            tx.writeSimpleField(id2, 2, id3, true);
            tx.writeSimpleField(id3, 2, id1, true);
            Assert.assertEquals(tx.readSimpleField(id1, 2, true), id2);
            Assert.assertEquals(tx.readSimpleField(id2, 2, true), id3);
            Assert.assertEquals(tx.readSimpleField(id3, 2, true), id1);

            try {
                tx.delete(id2);
                Assert.assertNotEquals(inverseDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(inverseDelete, DeleteAction.EXCEPTION);
            }
            switch (inverseDelete) {
            case IGNORE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2, true), id2);
                Assert.assertEquals(tx.readSimpleField(id3, 2, true), id1);
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2, true), id2);
                Assert.assertEquals(tx.readSimpleField(id2, 2, true), id3);
                Assert.assertEquals(tx.readSimpleField(id3, 2, true), id1);
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2, true), null);
                Assert.assertEquals(tx.readSimpleField(id3, 2, true), id1);
                break;
            case DELETE:
                Assert.assertFalse(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertFalse(tx.exists(id3));
                break;
            default:
                assert false;
                break;
            }

            tx.rollback();

        // Set

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            NavigableSet<ObjId> set = (NavigableSet<ObjId>)tx.readSetField(id1, 10, true);
            set.add(id1);
            set.add(id2);
            set.add(id3);
            TestSupport.checkSet(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(inverseDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(inverseDelete, DeleteAction.EXCEPTION);
            }
            switch (inverseDelete) {
            case IGNORE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(set, Sets.newTreeSet(Arrays.asList(id1, id3)));
                break;
            case DELETE:
                Assert.assertFalse(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                break;
            default:
                assert false;
                break;
            }

            tx.rollback();

        // List

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            List<ObjId> list = (List<ObjId>)tx.readListField(id1, 11, true);
            list.add(id1);
            list.add(id2);
            list.add(id3);
            Assert.assertEquals(list, Lists.newArrayList(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(inverseDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(inverseDelete, DeleteAction.EXCEPTION);
            }
            switch (inverseDelete) {
            case IGNORE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(list, Lists.newArrayList(Arrays.asList(id1, id2, id3)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(list, Lists.newArrayList(Arrays.asList(id1, id2, id3)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(list, Lists.newArrayList(Arrays.asList(id1, id3)));
                break;
            case DELETE:
                Assert.assertFalse(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                break;
            default:
                assert false;
                break;
            }

            tx.rollback();

        // Map Key

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            NavigableMap<ObjId, Integer> map1 = (NavigableMap<ObjId, Integer>)tx.readMapField(id1, 12, true);
            map1.put(id1, 123);
            map1.put(id2, 456);
            map1.put(id3, 789);
            Assert.assertEquals(map1.get(id2), (Integer)456);
            TestSupport.checkSet(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
            TestSupport.checkSet(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(inverseDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(inverseDelete, DeleteAction.EXCEPTION);
            }
            switch (inverseDelete) {
            case IGNORE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                TestSupport.checkSet(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                TestSupport.checkSet(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id3)));
                TestSupport.checkSet(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 789)));
                break;
            case DELETE:
                Assert.assertFalse(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                break;
            default:
                assert false;
                break;
            }

            tx.rollback();

        // Map Value

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            NavigableMap<Integer, ObjId> map2 = (NavigableMap<Integer, ObjId>)tx.readMapField(id1, 13, true);
            map2.put(123, id1);
            map2.put(456, id2);
            map2.put(789, id3);
            map2.put(333, id2);
            Assert.assertEquals(map2.get(456), id2);
            Assert.assertEquals(map2.get(333), id2);
            TestSupport.checkSet(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
            TestSupport.checkSet(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(inverseDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(inverseDelete, DeleteAction.EXCEPTION);
            }
            switch (inverseDelete) {
            case IGNORE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
                TestSupport.checkSet(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
                TestSupport.checkSet(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                TestSupport.checkSet(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 789)));
                TestSupport.checkSet(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id3)));
                break;
            case DELETE:
                Assert.assertFalse(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                break;
            default:
                assert false;
                break;
            }

            tx.rollback();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInverseDeleteUpdate() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        assert DeleteAction.IGNORE.ordinal() == 0;
        assert DeleteAction.EXCEPTION.ordinal() == 1;
        assert DeleteAction.UNREFERENCE.ordinal() == 2;
        assert DeleteAction.DELETE.ordinal() == 3;

        final SchemaModel[] schemas = new SchemaModel[4];
        for (DeleteAction inverseDelete : DeleteAction.values()) {
            schemas[inverseDelete.ordinal()] = SchemaModel.fromXML(
              new ByteArrayInputStream(XML_TEMPLATE.replaceAll("@INVERSE_DELETE@",
                inverseDelete.name()).getBytes(StandardCharsets.UTF_8)));
        }

        // Create target object and friends, with all references set to inverseDelete=IGNORE
        //  - other1 and other2 have various references to target and each other
        //  - target has only references to itself
        ObjId target;
        ObjId other1;
        ObjId other2;
        Transaction tx = db.createTransaction(schemas[0], 1, true);
        target = tx.create(1);
        tx.writeSimpleField(target, 3, "target", false);
        other1 = tx.create(1);
        tx.writeSimpleField(other1, 3, "other1", false);
        tx.writeSimpleField(other1, 2, target, false);
        other2 = tx.create(1);
        tx.writeSimpleField(other2, 3, "other2", false);
        ((NavigableSet<ObjId>)tx.readSetField(target, 10, true)).add(target);
        ((NavigableSet<ObjId>)tx.readSetField(other2, 10, true)).add(target);
        ((NavigableSet<ObjId>)tx.readSetField(other2, 10, true)).add(other1);
        ((List<ObjId>)tx.readListField(target, 11, false)).add(target);
        ((List<ObjId>)tx.readListField(target, 11, false)).add(target);
        ((List<ObjId>)tx.readListField(other1, 11, false)).add(target);
        ((List<ObjId>)tx.readListField(other1, 11, false)).add(other2);
        ((List<ObjId>)tx.readListField(other1, 11, false)).add(target);
        ((NavigableMap<ObjId, Integer>)tx.readMapField(target, 12, false)).put(target, 99);
        ((NavigableMap<ObjId, Integer>)tx.readMapField(other2, 12, false)).put(other1, 123);
        ((NavigableMap<ObjId, Integer>)tx.readMapField(other2, 12, false)).put(target, 456);
        ((NavigableMap<Integer, ObjId>)tx.readMapField(target, 13, false)).put(88, target);
        ((NavigableMap<Integer, ObjId>)tx.readMapField(other1, 13, false)).put(789, target);
        ((NavigableMap<Integer, ObjId>)tx.readMapField(other1, 13, false)).put(123, other2);
        ((NavigableMap<Integer, ObjId>)tx.readMapField(other1, 13, false)).put(636, target);
        tx.commit();

        // Create referring objects and schema versions
        //  - Each referring objects refers to target in every field
        ObjId[] referrers = new ObjId[4];
        for (DeleteAction inverseDelete : DeleteAction.values()) {
            final int i = inverseDelete.ordinal();
            tx = db.createTransaction(schemas[i], i + 1, true);
            referrers[i] = tx.create(1);
            tx.writeSimpleField(referrers[i], 3, "referrers[" + i + "]", false);
            tx.writeSimpleField(referrers[i], 2, target, false);
            ((NavigableSet<ObjId>)tx.readSetField(referrers[i], 10, true)).add(target);
            ((List<ObjId>)tx.readListField(referrers[i], 11, false)).add(target);
            ((List<ObjId>)tx.readListField(referrers[i], 11, false)).add(referrers[i]);
            ((List<ObjId>)tx.readListField(referrers[i], 11, false)).add(target);
            ((NavigableMap<ObjId, Integer>)tx.readMapField(referrers[i], 12, false)).put(target, 343);
            ((NavigableMap<Integer, ObjId>)tx.readMapField(referrers[i], 13, false)).put(452, target);
            ((NavigableMap<Integer, ObjId>)tx.readMapField(referrers[i], 13, false)).put(453, referrers[i]);
            ((NavigableMap<Integer, ObjId>)tx.readMapField(referrers[i], 13, false)).put(454, target);
            tx.commit();
        }

        // Try to delete target - should fail due to referrers[1] inverseDelete=EXCEPTION
        tx = db.createTransaction(schemas[0], 1, false);
        try {
            tx.delete(target);
            assert false : "expected ReferencedObjectException";
        } catch (ReferencedObjectException e) {
            this.log.info("got expected {}", e.toString());
        }

        // Delete referrers[1] and try again - this time it should succeed
        tx.delete(referrers[DeleteAction.EXCEPTION.ordinal()]);
        tx.delete(target);

        // Verify all indexes were updated properly
        TestSupport.checkMap(tx.queryVersion().asMap(), buildMap(
          1, buildSet(other1, other2, referrers[0]),
          3, buildSet(referrers[2])));
        TestSupport.checkMap(tx.queryIndex(2).asMap(), buildMap(
          target, buildSet(referrers[0], other1),
          null, buildSet(other2, referrers[2])));
        TestSupport.checkMap(tx.queryIndex(20).asMap(), buildMap(
          target, buildSet(referrers[0], other2),
          other1, buildSet(other2)));
        TestSupport.checkSet(tx.queryListElementIndex(21).asSet(), buildSet(
          new Tuple3<>(target, other1, 0),
          new Tuple3<>(target, other1, 2),
          new Tuple3<>(target, referrers[0], 0),
          new Tuple3<>(target, referrers[0], 2),
          new Tuple3<>(other2, other1, 1),
          new Tuple3<>(referrers[0], referrers[0], 1),
          new Tuple3<>(referrers[2], referrers[2], 0)));
        TestSupport.checkMap(tx.queryIndex(22).asMap(), buildMap(
          target, buildSet(other2, referrers[0]),
          other1, buildSet(other2)));
        TestSupport.checkSet(tx.queryMapValueIndex(25).asSet(), buildSet(
          new Tuple3<>(target, other1, 789),
          new Tuple3<>(target, other1, 636),
          new Tuple3<>(target, referrers[0], 452),
          new Tuple3<>(target, referrers[0], 454),
          new Tuple3<>(other2, other1, 123),
          new Tuple3<>(referrers[0], referrers[0], 453),
          new Tuple3<>(referrers[2], referrers[2], 453)));

        tx.commit();
    }
}
