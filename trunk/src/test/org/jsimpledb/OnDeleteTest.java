
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OnDeleteTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testOnDelete() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase(100, 200);
        final Database db = new Database(kvstore);

        final String xmlTemplate =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <Object name=\"Foo\" storageId=\"1\">\n"
          + "    <ReferenceField name=\"ref\" storageId=\"2\" onDelete=\"@ONDELETE@\"/>\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <ReferenceField storageId=\"20\" onDelete=\"@ONDELETE@\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <ReferenceField storageId=\"21\" onDelete=\"@ONDELETE@\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map1\" storageId=\"12\">\n"
          + "        <ReferenceField storageId=\"22\" onDelete=\"@ONDELETE@\"/>\n"
          + "        <SimpleField type=\"int\" storageId=\"23\"/>\n"
          + "    </MapField>"
          + "    <MapField name=\"map2\" storageId=\"13\">\n"
          + "        <SimpleField type=\"int\" storageId=\"24\"/>\n"
          + "        <ReferenceField storageId=\"25\" onDelete=\"@ONDELETE@\"/>\n"
          + "    </MapField>"
          + "  </Object>\n"
          + "</Schema>\n";

        for (DeleteAction onDelete : DeleteAction.values()) {
            final String xml = xmlTemplate.replaceAll("@ONDELETE@", onDelete.name());
            final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes("UTF-8")));

            ObjId id1;
            ObjId id2;
            ObjId id3;
            Transaction tx;

        // Referencefield

            tx = db.createTransaction(schema, 1, true);

            id1 = tx.create(1);
            id2 = tx.create(1);
            id3 = tx.create(1);

            tx.writeSimpleField(id1, 2, id2);
            tx.writeSimpleField(id2, 2, id3);
            tx.writeSimpleField(id3, 2, id1);
            Assert.assertEquals(tx.readSimpleField(id1, 2), id2);
            Assert.assertEquals(tx.readSimpleField(id2, 2), id3);
            Assert.assertEquals(tx.readSimpleField(id3, 2), id1);

            try {
                tx.delete(id2);
                Assert.assertNotEquals(onDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(onDelete, DeleteAction.EXCEPTION);
            }
            switch (onDelete) {
            case NOTHING:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2), id2);
                Assert.assertEquals(tx.readSimpleField(id3, 2), id1);
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2), id2);
                Assert.assertEquals(tx.readSimpleField(id2, 2), id3);
                Assert.assertEquals(tx.readSimpleField(id3, 2), id1);
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(tx.readSimpleField(id1, 2), null);
                Assert.assertEquals(tx.readSimpleField(id3, 2), id1);
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

            NavigableSet<ObjId> set = (NavigableSet<ObjId>)tx.readSetField(id1, 10);
            set.add(id1);
            set.add(id2);
            set.add(id3);
            Assert.assertEquals(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(onDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(onDelete, DeleteAction.EXCEPTION);
            }
            switch (onDelete) {
            case NOTHING:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(set, Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(set, Sets.newTreeSet(Arrays.asList(id1, id3)));
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

            List<ObjId> list = (List<ObjId>)tx.readListField(id1, 11);
            list.add(id1);
            list.add(id2);
            list.add(id3);
            Assert.assertEquals(list, Lists.newArrayList(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(onDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(onDelete, DeleteAction.EXCEPTION);
            }
            switch (onDelete) {
            case NOTHING:
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

            NavigableMap<ObjId, Integer> map1 = (NavigableMap<ObjId, Integer>)tx.readMapField(id1, 12);
            map1.put(id1, 123);
            map1.put(id2, 456);
            map1.put(id3, 789);
            Assert.assertEquals(map1.get(id2), (Integer)456);
            Assert.assertEquals(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
            Assert.assertEquals(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(onDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(onDelete, DeleteAction.EXCEPTION);
            }
            switch (onDelete) {
            case NOTHING:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                Assert.assertEquals(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                Assert.assertEquals(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 456, 789)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map1.keySet(), Sets.newTreeSet(Arrays.asList(id1, id3)));
                Assert.assertEquals(Sets.newTreeSet(map1.values()), Sets.newTreeSet(Arrays.asList(123, 789)));
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

            NavigableMap<Integer, ObjId> map2 = (NavigableMap<Integer, ObjId>)tx.readMapField(id1, 13);
            map2.put(123, id1);
            map2.put(456, id2);
            map2.put(789, id3);
            map2.put(333, id2);
            Assert.assertEquals(map2.get(456), id2);
            Assert.assertEquals(map2.get(333), id2);
            Assert.assertEquals(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
            Assert.assertEquals(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));

            try {
                tx.delete(id2);
                Assert.assertNotEquals(onDelete, DeleteAction.EXCEPTION);
            } catch (ReferencedObjectException e) {
                Assert.assertEquals(onDelete, DeleteAction.EXCEPTION);
            }
            switch (onDelete) {
            case NOTHING:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
                Assert.assertEquals(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case EXCEPTION:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertTrue(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 456, 789, 333)));
                Assert.assertEquals(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id2, id3)));
                break;
            case UNREFERENCE:
                Assert.assertTrue(tx.exists(id1));
                Assert.assertFalse(tx.exists(id2));
                Assert.assertTrue(tx.exists(id3));
                Assert.assertEquals(map2.keySet(), Sets.newTreeSet(Arrays.asList(123, 789)));
                Assert.assertEquals(Sets.newTreeSet(map2.values()), Sets.newTreeSet(Arrays.asList(id1, id3)));
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
}

