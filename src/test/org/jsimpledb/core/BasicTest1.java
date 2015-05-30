
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.ByteArrayInputStream;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicTest1 extends TestSupport {

    @Test
    public void testPrimitiveFields() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"20\"/>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema1, 1, true);
        //this.showKV(tx, "testPrimitiveFields: 1");
        ObjId id = tx.create(1);
        ObjId id2 = tx.create(20);
        Assert.assertTrue(tx.exists(id));
        Assert.assertTrue(tx.exists(id2));
        //this.showKV(tx, "testPrimitiveFields: 2");
        Assert.assertEquals(tx.getSchemaVersion(id), 1);
        Assert.assertEquals(tx.getAll(1), Collections.singleton(id));
        Assert.assertEquals(tx.getAll(20), Collections.singleton(id2));
        Assert.assertEquals(tx.readSimpleField(id, 2, true), 0);
        tx.writeSimpleField(id, 2, 123456, true);
        //this.showKV(tx, "testPrimitiveFields: 3");
        Assert.assertEquals(tx.readSimpleField(id, 2, true), 123456);
        tx.commit();

        tx = db.createTransaction(schema1, 1, true);
        Assert.assertTrue(tx.getAll(1).equals(Collections.singleton(id)), tx.getAll(1) + " != " + Collections.singleton(id));
        Assert.assertEquals(tx.readSimpleField(id, 2, true), 123456);
        tx.commit();

        tx = db.createTransaction(schema1, 1, true);
        tx.writeSimpleField(id, 2, 987654, true);
        tx.rollback();

        tx = db.createTransaction(schema1, 1, true);
        Assert.assertEquals(tx.readSimpleField(id, 2, true), 123456);
        tx.commit();

        tx = db.createTransaction(schema1, 1, true);
        Assert.assertTrue(tx.exists(id));
        Assert.assertTrue(tx.delete(id));
        Assert.assertFalse(tx.exists(id));
        Assert.assertFalse(tx.delete(id));
        Assert.assertFalse(tx.exists(id));
        Assert.assertTrue(tx.getAll(1).equals(Collections.emptySet()));
        try {
            tx.getSchemaVersion(id);
            assert false : "expected " + id + " to be deleted";
        } catch (DeletedObjectException e) {
            // expected
        }
        tx.rollback();

        // Change all the names - should still work

        final SchemaModel schema1a = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"AAAA\" storageId=\"1\">\n"
          + "    <SimpleField name=\"BBB\" type=\"int\" storageId=\"2\"/>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"CCC\" storageId=\"20\"/>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));
        tx = db.createTransaction(schema1a, 1, false);
        tx.rollback();

        // Do schema update that adds new fields

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "    <SimpleField name=\"z\" type=\"boolean\" storageId=\"3\"/>\n"
          + "    <SimpleField name=\"b\" type=\"byte\" storageId=\"4\"/>\n"
          + "    <SimpleField name=\"c\" type=\"char\" storageId=\"5\"/>\n"
          + "    <SimpleField name=\"s\" type=\"short\" storageId=\"6\"/>\n"
          + "    <SimpleField name=\"f\" type=\"float\" storageId=\"7\"/>\n"
          + "    <SimpleField name=\"j\" type=\"long\" storageId=\"8\"/>\n"
          + "    <SimpleField name=\"d\" type=\"double\" storageId=\"9\"/>\n"
          + "    <SimpleField name=\"str\" type=\"java.lang.String\" storageId=\"10\"/>\n"
          + "    <ReferenceField name=\"r\" storageId=\"11\"/>\n"
          + "    <SimpleField name=\"v\" type=\"java.lang.Void\" storageId=\"12\"/>\n"
          + "    <SimpleField name=\"date\" type=\"java.util.Date\" storageId=\"13\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        tx = db.createTransaction(schema2, 2, true);
        //this.showKV(tx, "testPrimitiveFields[2]: 1");
        Assert.assertEquals(tx.getSchemaVersion(id), 1);

        final int[] oldValue = new int[1];
        tx.addVersionChangeListener(new VersionChangeListener() {
            @Override
            public void onVersionChange(Transaction tx, ObjId id,
              int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues) {
                //log.info("version change: " + oldVersion + " -> " + newVersion + " oldFields=" + oldFieldValues);
                oldValue[0] = (Integer)oldFieldValues.get(2);
                tx.writeSimpleField(id, 6, (short)4444, true);
            }
        });

        Assert.assertEquals(tx.readSimpleField(id, 3, true), false);
        //this.showKV(tx, "testPrimitiveFields[2]: 2");
        Assert.assertEquals(tx.readSimpleField(id, 4, true), (byte)0);
        Assert.assertEquals(tx.readSimpleField(id, 5, true), (char)0);
        Assert.assertEquals(tx.readSimpleField(id, 6, true), (short)4444);
        Assert.assertEquals(tx.readSimpleField(id, 7, true), (float)0);
        Assert.assertEquals(tx.readSimpleField(id, 8, true), (long)0);
        Assert.assertEquals(tx.readSimpleField(id, 9, true), (double)0);
        Assert.assertEquals(tx.readSimpleField(id, 10, true), null);
        Assert.assertEquals(tx.readSimpleField(id, 11, true), null);
        Assert.assertEquals(tx.readSimpleField(id, 12, true), null);
        Assert.assertEquals(tx.readSimpleField(id, 13, true), null);

        Assert.assertEquals(tx.getSchemaVersion(id), 2);
        Assert.assertEquals(oldValue[0], 123456);

        for (int sid = 2; sid <= 9; sid++) {
            try {
                tx.writeSimpleField(id, sid, null, true);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        }

        final Date now = new Date();
        tx.writeSimpleField(id, 13, now, true);
        Assert.assertEquals(tx.readSimpleField(id, 13, true), now);

        tx.rollback();
    }

    @Test
    public void testPrimitiveArrays() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" type=\"int[]\" storageId=\"2\"/>\n"
          + "    <SimpleField name=\"z\" type=\"boolean[]\" storageId=\"3\"/>\n"
          + "    <SimpleField name=\"b\" type=\"byte[]\" storageId=\"4\"/>\n"
          + "    <SimpleField name=\"c\" type=\"char[]\" storageId=\"5\"/>\n"
          + "    <SimpleField name=\"s\" type=\"short[]\" storageId=\"6\"/>\n"
          + "    <SimpleField name=\"f\" type=\"float[]\" storageId=\"7\"/>\n"
          + "    <SimpleField name=\"j\" type=\"long[]\" storageId=\"8\"/>\n"
          + "    <SimpleField name=\"d\" type=\"double[]\" storageId=\"9\"/>\n"
          + "    <SimpleField name=\"str\" type=\"java.lang.String[]\" storageId=\"10\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));
        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema, 1, true);
        //this.showKV(tx, "testPrimitiveFields: 1");
        final ObjId id = tx.create(1);

        final int[] i = new int[] { this.random.nextInt(), this.random.nextInt(), this.random.nextInt() };
        final boolean[] z = new boolean[] { this.random.nextBoolean(), this.random.nextBoolean(), this.random.nextBoolean() };
        final byte[] b = new byte[this.random.nextInt(10)];
        this.random.nextBytes(b);
        final char[] c = new char[] { (char)this.random.nextInt(), (char)this.random.nextInt(), (char)this.random.nextInt() };
        final short[] s = new short[] { (short)this.random.nextInt(), (short)this.random.nextInt(), (short)this.random.nextInt() };
        final float[] f = new float[] { this.random.nextFloat(), this.random.nextFloat(), this.random.nextFloat() };
        final long[] j = new long[] { this.random.nextLong(), this.random.nextLong(), this.random.nextLong() };
        final double[] d = new double[] { this.random.nextDouble(), this.random.nextDouble(), this.random.nextDouble() };
        final String[] str = new String[] { "abc", "def", "", "\ud0d0" };

        tx.writeSimpleField(id, 2, i, false);
        tx.writeSimpleField(id, 3, z, false);
        tx.writeSimpleField(id, 4, b, false);
        tx.writeSimpleField(id, 5, c, false);
        tx.writeSimpleField(id, 6, s, false);
        tx.writeSimpleField(id, 7, f, false);
        tx.writeSimpleField(id, 8, j, false);
        tx.writeSimpleField(id, 9, d, false);
        tx.writeSimpleField(id, 10, str, false);

        Assert.assertTrue(Arrays.equals((int[])tx.readSimpleField(id, 2, false), i));
        Assert.assertTrue(Arrays.equals((boolean[])tx.readSimpleField(id, 3, false), z));
        Assert.assertTrue(Arrays.equals((byte[])tx.readSimpleField(id, 4, false), b));
        Assert.assertTrue(Arrays.equals((char[])tx.readSimpleField(id, 5, false), c));
        Assert.assertTrue(Arrays.equals((short[])tx.readSimpleField(id, 6, false), s));
        Assert.assertTrue(Arrays.equals((float[])tx.readSimpleField(id, 7, false), f));
        Assert.assertTrue(Arrays.equals((long[])tx.readSimpleField(id, 8, false), j));
        Assert.assertTrue(Arrays.equals((double[])tx.readSimpleField(id, 9, false), d));
        Assert.assertTrue(Arrays.equals((String[])tx.readSimpleField(id, 10, false), str));

        tx.rollback();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexDelete() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <SimpleField type=\"int\" storageId=\"20\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <SimpleField type=\"int\" storageId=\"21\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"12\">\n"
          + "        <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"23\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx = db.createTransaction(schema, 1, true);

        ObjId id = tx.create(1);

        NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id, 10, true);
        set.add(123);
        set.add(456);
        set.add(789);

        List<Integer> list = (List<Integer>)tx.readListField(id, 11, true);
        list.add(234);
        list.add(567);
        list.add(890);

        NavigableMap<Integer, String> map = (NavigableMap<Integer, String>)tx.readMapField(id, 12, true);
        map.put(987, "foo");
        map.put(654, "bar");
        map.put(321, "jan");

        Assert.assertFalse(set.isEmpty());
        Assert.assertFalse(list.isEmpty());
        Assert.assertFalse(map.isEmpty());

        tx.delete(id);

        Assert.assertTrue(set.isEmpty());
        Assert.assertEquals(set, Sets.<Integer>newHashSet());
        Assert.assertTrue(list.isEmpty());
        Assert.assertEquals(list, Lists.<Integer>newArrayList());
        Assert.assertTrue(map.isEmpty());
        Assert.assertEquals(map, Collections.<Integer, String>emptyMap());

        try {
            set.add(123);
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
        try {
            list.add(234);
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
        try {
            map.put(987, "foo");
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            set.remove(123);
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
        try {
            map.remove(987);
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }

        try {
            set.clear();
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
        try {
            list.clear();
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
        try {
            map.clear();
            assert false;
        } catch (DeletedObjectException e) {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetField() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <SimpleField type=\"int\" storageId=\"20\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"21\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"12\">\n"
          + "        <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"23\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));
        Transaction tx = db.createTransaction(schema, 1, true);

        ObjId id = tx.create(1);
        NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id, 10, true);
        Assert.assertEquals(tx.readSetField(id, 10, true), Collections.emptySet());
        set.add(456);
        set.add(123);
        set.add(789);
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        try {
            set.add(null);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            tx.readListField(id, 10, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {
            tx.readMapField(id, 10, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }

    // Set

        Assert.assertTrue(set.equals(set));
        Assert.assertFalse(set.equals(new Object()));

        Assert.assertEquals(set.size(), 3);
        Assert.assertFalse(set.isEmpty());

        Assert.assertEquals(Lists.newArrayList(set.iterator()), Lists.newArrayList(123, 456, 789));

        Assert.assertEquals(set.toArray(), new Object[] { 123, 456, 789 });
        Assert.assertEquals(set.toArray(new Integer[set.size()]), new Integer[] { 123, 456, 789 });

        Assert.assertTrue(set.equals(Sets.newHashSet(123, 456, 789)));
        Assert.assertFalse(set.equals(Sets.newHashSet(33, 123, 456, 789)));
        Assert.assertFalse(set.equals(Sets.newHashSet(123, 456, 789, 1234)));
        Assert.assertFalse(set.equals(Sets.newHashSet(33, 123, 456, 789, 1234)));
        Assert.assertFalse(set.equals(Sets.newHashSet(123, 789)));
        Assert.assertFalse(set.equals(Sets.newHashSet(456)));

        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        Assert.assertEquals(Sets.newHashSet(123, 456, 789), set);
        Assert.assertEquals(Sets.newHashSet(123, 456, 789), set);

        Assert.assertTrue(set.containsAll(Sets.newHashSet()));
        Assert.assertTrue(set.containsAll(Sets.newHashSet(789)));
        Assert.assertTrue(set.containsAll(Sets.newHashSet(123, 456, 789)));
        Assert.assertFalse(set.containsAll(Sets.newHashSet(543)));

        Assert.assertTrue(set.contains(123));
        Assert.assertTrue(set.contains(456));
        Assert.assertTrue(set.contains(789));
        Assert.assertFalse(set.contains(Integer.MIN_VALUE));
        Assert.assertFalse(set.contains(0));
        Assert.assertFalse(set.contains(122));
        Assert.assertFalse(set.contains(457));
        Assert.assertFalse(set.contains(Integer.MAX_VALUE));
        Assert.assertFalse(set.contains("not an integer"));
        Assert.assertFalse(set.contains(123L));
        Assert.assertFalse(set.contains(123.0f));

    // SortedSet

        Assert.assertEquals(set.first(), (Integer)123);
        try {
            set.headSet(123).first();
            assert false;
        } catch (NoSuchElementException e) {
            // expected
        }
        Assert.assertEquals(set.last(), (Integer)789);
        try {
            set.tailSet(790).last();
            assert false;
        } catch (NoSuchElementException e) {
            // expected
        }

        Assert.assertTrue(set.comparator().compare(87454, -23413) > 0);
        Assert.assertTrue(set.comparator().compare(-23413, 743234) < 0);
        Assert.assertTrue(set.comparator().compare(12345, 12345) == 0);

        Assert.assertEquals(set.headSet(123), Sets.newHashSet());
        Assert.assertEquals(set.headSet(124), Sets.newHashSet(123));
        Assert.assertEquals(set.headSet(456), Sets.newHashSet(123));
        Assert.assertEquals(set.headSet(457), Sets.newHashSet(123, 456));
        Assert.assertEquals(set.headSet(789), Sets.newHashSet(123, 456));
        Assert.assertEquals(set.headSet(790), set);

        try {
            set.headSet(456).headSet(789);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            set.tailSet(456).tailSet(123);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            set.subSet(456, 789).subSet(123, 789);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            set.subSet(123, 456).subSet(123, 789);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        Assert.assertEquals(set.tailSet(123), set);
        Assert.assertEquals(set.tailSet(124), Sets.newHashSet(456, 789));
        Assert.assertEquals(set.tailSet(456), Sets.newHashSet(456, 789));
        Assert.assertEquals(set.tailSet(457), Sets.newHashSet(789));
        Assert.assertEquals(set.tailSet(789), Sets.newHashSet(789));
        Assert.assertEquals(set.tailSet(790), Sets.newHashSet());

        Assert.assertEquals(set.subSet(456, 457), Sets.newHashSet(456));
        Assert.assertEquals(set.subSet(456, 456), Sets.newHashSet());

        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        NavigableSet<Integer> subSet = set.subSet(200, true, 800, true);
        Assert.assertFalse(subSet.contains(123));
        Assert.assertTrue(subSet.contains(456));
        Assert.assertTrue(subSet.contains(789));
        Assert.assertEquals(Lists.newArrayList(subSet.iterator()), Lists.newArrayList(456, 789));
        Assert.assertEquals(Lists.newArrayList(subSet.descendingIterator()), Lists.newArrayList(789, 456));
        Assert.assertFalse(subSet.contains(200));
        Assert.assertFalse(subSet.contains(800));
        try {
            subSet.add(100);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        subSet.add(300);
        Assert.assertEquals(set, Sets.newHashSet(123, 300, 456, 789));
        Assert.assertEquals(subSet, Sets.newHashSet(300, 456, 789));
        subSet.clear();
        Assert.assertEquals(subSet, Sets.newHashSet());
        Assert.assertEquals(set, Sets.newHashSet(123));
        subSet.add(456);
        Assert.assertEquals(set, Sets.newHashSet(123, 456));
        Assert.assertEquals(subSet, Sets.newHashSet(456));
        subSet.add(789);
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        Assert.assertEquals(subSet, Sets.newHashSet(456, 789));
        try {
            subSet.add(999);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));
        set.add(890);
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789, 890));
        subSet.remove(890);
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789, 890));
        subSet.add(345);
        Assert.assertEquals(set, Sets.newHashSet(123, 345, 456, 789, 890));
        Iterator<Integer> i = set.descendingSet().iterator();
        i.next();
        i.next();
        i.next();
        i.next();
        i.remove();
        try {
            i.remove();
        } catch (IllegalStateException e) {
            // expected
        }
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789, 890));
        Assert.assertEquals(set.descendingSet().pollFirst(), (Integer)890);
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));

    // NavigableSet

        Assert.assertEquals(set.headSet(123, false), Sets.newHashSet());
        Assert.assertEquals(set.headSet(123, true), Sets.newHashSet(123));
        Assert.assertEquals(set.headSet(600, false), Sets.newHashSet(123, 456));
        Assert.assertEquals(set.headSet(789, false), Sets.newHashSet(123, 456));
        Assert.assertEquals(set.headSet(789, true), Sets.newHashSet(123, 456, 789));

        Assert.assertEquals(set.subSet(123, false, 789, false), Sets.newHashSet(456));
        Assert.assertEquals(set.subSet(123, false, 789, true), Sets.newHashSet(456, 789));
        Assert.assertEquals(set.subSet(123, true, 789, false), Sets.newHashSet(123, 456));
        Assert.assertEquals(set.subSet(123, true, 789, true), Sets.newHashSet(123, 456, 789));

        Assert.assertEquals(Lists.newArrayList(set.descendingIterator()), Lists.newArrayList(789, 456, 123));
        Assert.assertEquals(Lists.newArrayList(set.subSet(123, 789).iterator()), Lists.newArrayList(123, 456));
        Assert.assertEquals(Lists.newArrayList(((NavigableSet<?>)set.subSet(123, 789)).descendingIterator()),
          Lists.newArrayList(456, 123));
        Assert.assertEquals(Lists.newArrayList(set.descendingIterator()), Lists.newArrayList(set.descendingSet().iterator()));

        Assert.assertEquals(set.descendingSet(), set);

        Assert.assertEquals(set.ceiling(456), (Integer)456);
        Assert.assertEquals(set.ceiling(457), (Integer)789);
        Assert.assertEquals(set.ceiling(789), (Integer)789);
        Assert.assertEquals(set.ceiling(790), null);

        Assert.assertEquals(set.subSet(300, true, 500, true).ceiling(200), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).ceiling(300), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).ceiling(456), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).ceiling(500), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).ceiling(600), null);

        Assert.assertEquals(set.floor(456), (Integer)456);
        Assert.assertEquals(set.floor(455), (Integer)123);
        Assert.assertEquals(set.floor(123), (Integer)123);
        Assert.assertEquals(set.floor(122), null);

        Assert.assertEquals(set.subSet(300, true, 500, true).floor(200), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).floor(300), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).floor(456), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).floor(500), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).floor(600), (Integer)456);

        Assert.assertEquals(set.higher(455), (Integer)456);
        Assert.assertEquals(set.higher(456), (Integer)789);
        Assert.assertEquals(set.higher(788), (Integer)789);
        Assert.assertEquals(set.higher(789), null);

        Assert.assertEquals(set.subSet(300, true, 500, true).higher(200), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).higher(300), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).higher(456), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).higher(500), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).higher(600), null);

        Assert.assertEquals(set.lower(457), (Integer)456);
        Assert.assertEquals(set.lower(456), (Integer)123);
        Assert.assertEquals(set.lower(124), (Integer)123);
        Assert.assertEquals(set.lower(123), null);

        Assert.assertEquals(set.subSet(300, true, 500, true).lower(200), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).lower(300), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).lower(456), null);
        Assert.assertEquals(set.subSet(300, true, 500, true).lower(500), (Integer)456);
        Assert.assertEquals(set.subSet(300, true, 500, true).lower(600), (Integer)456);

        Assert.assertEquals(set.subSet(456, false, 789, false).ceiling(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).ceiling(456), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, true).ceiling(456), (Integer)789);
        Assert.assertEquals(set.subSet(456, true, 789, true).ceiling(456), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, false).ceiling(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).ceiling(789), null);
        Assert.assertEquals(set.subSet(456, false, 789, true).ceiling(789), (Integer)789);
        Assert.assertEquals(set.subSet(456, true, 789, true).ceiling(789), (Integer)789);

        Assert.assertEquals(set.subSet(456, false, 789, false).higher(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).higher(456), null);
        Assert.assertEquals(set.subSet(456, false, 789, true).higher(456), (Integer)789);
        Assert.assertEquals(set.subSet(456, true, 789, true).higher(456), (Integer)789);
        Assert.assertEquals(set.subSet(456, false, 789, false).higher(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).higher(789), null);
        Assert.assertEquals(set.subSet(456, false, 789, true).higher(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, true).higher(789), null);

        Assert.assertEquals(set.subSet(456, false, 789, false).floor(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).floor(456), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, true).floor(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, true).floor(456), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, false).floor(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).floor(789), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, true).floor(789), (Integer)789);
        Assert.assertEquals(set.subSet(456, true, 789, true).floor(789), (Integer)789);

        Assert.assertEquals(set.subSet(456, false, 789, false).lower(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).lower(456), null);
        Assert.assertEquals(set.subSet(456, false, 789, true).lower(456), null);
        Assert.assertEquals(set.subSet(456, true, 789, true).lower(456), null);
        Assert.assertEquals(set.subSet(456, false, 789, false).lower(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, false).lower(789), (Integer)456);
        Assert.assertEquals(set.subSet(456, false, 789, true).lower(789), null);
        Assert.assertEquals(set.subSet(456, true, 789, true).lower(789), (Integer)456);

        Assert.assertEquals(Lists.newArrayList(set), Lists.newArrayList(123, 456, 789));
        set.subSet(400, 500).clear();
        Assert.assertEquals(Lists.newArrayList(set), Lists.newArrayList(123, 789));
        set.add(456);
        Assert.assertEquals(Lists.newArrayList(set), Lists.newArrayList(123, 456, 789));

        for (boolean b1 : new boolean[] { false, true }) {
            for (boolean b2 : new boolean[] { false, true }) {
                final boolean empty = !b1 && !b2;
                Assert.assertEquals(set.subSet(456, b1, 789, b2).ceiling(455), b1 ? (Integer)456 : b2 ? (Integer)789 : null);
                Assert.assertEquals(set.subSet(456, b1, 789, b2).ceiling(790), null);

                Assert.assertEquals(set.subSet(456, b1, 789, b2).higher(455), b1 ? (Integer)456 : b2 ? (Integer)789 : null);
                Assert.assertEquals(set.subSet(456, b1, 789, b2).higher(790), null);

                Assert.assertEquals(set.subSet(456, b1, 789, b2).floor(455), null);
                Assert.assertEquals(set.subSet(456, b1, 789, b2).floor(790), b2 ? (Integer)789 : b1 ? (Integer)456 : null);

                Assert.assertEquals(set.subSet(456, b1, 789, b2).lower(455), null);
                Assert.assertEquals(set.subSet(456, b1, 789, b2).lower(790), b2 ? (Integer)789 : b1 ? (Integer)456 : null);
            }
        }

        Assert.assertEquals(set.pollFirst(), (Integer)123);
        Assert.assertEquals(set, Sets.newHashSet(456, 789));
        Assert.assertEquals(set.pollFirst(), (Integer)456);
        Assert.assertEquals(set, Sets.newHashSet(789));
        Assert.assertEquals(set.pollFirst(), (Integer)789);
        Assert.assertEquals(set, Sets.newHashSet());
        Assert.assertEquals(set.pollFirst(), null);

        set.addAll(Sets.newHashSet(123, 456, 789));
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));

        Assert.assertEquals(set.pollLast(), (Integer)789);
        Assert.assertEquals(set, Sets.newHashSet(123, 456));
        Assert.assertEquals(set.pollLast(), (Integer)456);
        Assert.assertEquals(set, Sets.newHashSet(123));
        Assert.assertEquals(set.pollLast(), (Integer)123);
        Assert.assertEquals(set, Sets.newHashSet());
        Assert.assertEquals(set.pollLast(), null);

        set.addAll(Sets.newHashSet(123, 456, 789));
        Assert.assertEquals(set, Sets.newHashSet(123, 456, 789));

        tx.rollback();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testListField() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <SimpleField type=\"int\" storageId=\"20\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"21\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"12\">\n"
          + "        <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"23\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));
        Transaction tx = db.createTransaction(schema, 1, true);

        ObjId id = tx.create(1);
        List<String> list = (List<String>)tx.readListField(id, 11, true);
        Assert.assertEquals(list, Collections.emptySet());
        Assert.assertEquals(Collections.emptyList(), list);
        Assert.assertEquals(Collections.emptyList().hashCode(), list.hashCode());
        Assert.assertTrue(list.isEmpty());
        Assert.assertEquals(list.size(), 0);
        Assert.assertTrue(Lists.newArrayList(list.iterator()).isEmpty());

        try {
            tx.readSetField(id, 11, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {
            tx.readMapField(id, 11, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }

        list.add("foo");
        Assert.assertEquals(list, Lists.newArrayList("foo"));
        Assert.assertEquals(Lists.newArrayList(list.iterator()), Lists.newArrayList("foo"));
        Assert.assertFalse(list.isEmpty());
        Assert.assertEquals(list.size(), 1);
        try {
            list.get(-1);
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        Assert.assertEquals(list.get(0), "foo");
        try {
            list.get(1);
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        list.add(null);
        Assert.assertEquals(list.size(), 2);
        Assert.assertEquals(list, Lists.newArrayList("foo", null));
        Assert.assertEquals(Lists.newArrayList("foo", null), list);
        Assert.assertEquals(list.hashCode(), Lists.newArrayList("foo", null).hashCode());
        Assert.assertEquals(Lists.newArrayList(list.iterator()), Lists.newArrayList("foo", null));

        Assert.assertTrue(list.equals(list));
        Assert.assertFalse(list.equals(new Object()));

        list.add(1, "bar");
        Assert.assertEquals(list.size(), 3);
        Assert.assertEquals(list, Lists.newArrayList("foo", "bar", null));

        try {
            list.add(4, "nope");
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        try {
            list.remove(3);
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        list.addAll(Lists.newArrayList("ee", "ff", "gg"));
        Assert.assertEquals(list.size(), 6);
        Assert.assertEquals(list, Lists.newArrayList("foo", "bar", null, "ee", "ff", "gg"));

        list.subList(3, 5).clear();
        Assert.assertEquals(list.size(), 4);
        Assert.assertEquals(list, Lists.newArrayList("foo", "bar", null, "gg"));

        Assert.assertEquals(list.indexOf("bar"), 1);
        Assert.assertEquals(list.indexOf("grill"), -1);
        Assert.assertEquals(list.indexOf(new Object()), -1);

        list.set(0, null);
        Assert.assertEquals(list.size(), 4);
        Assert.assertEquals(list, Lists.newArrayList(null, "bar", null, "gg"));

        Assert.assertEquals(list.indexOf(null), 0);
        Assert.assertEquals(list.lastIndexOf(null), 2);

        list.add("hhh");
        list.add("kkk");
        list.add(3, "jjj");
        Assert.assertEquals(list, Lists.newArrayList(null, "bar", null, "jjj", "gg", "hhh", "kkk"));

        list.subList(1, 4).clear();
        Assert.assertEquals(list.size(), 4);
        Assert.assertEquals(list, Lists.newArrayList(null, "gg", "hhh", "kkk"));

        list.set(2, "doh");
        Assert.assertEquals(list, Lists.newArrayList(null, "gg", "doh", "kkk"));

        try {
            list.set(4, "flo");
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        try {
            list.set(-1, "flo");
            assert false;
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        Assert.assertEquals(list, Lists.newArrayList(null, "gg", "doh", "kkk"));
        Assert.assertEquals(list.subList(1, 3), Lists.newArrayList("gg", "doh"));
        Assert.assertEquals(list.subList(1, 3).size(), 2);

        list.subList(1, 3).remove(0);
        Assert.assertEquals(list, Lists.newArrayList(null, "doh", "kkk"));
        Assert.assertEquals(list.subList(1, 3), Lists.newArrayList("doh", "kkk"));

        list.add(3, "fun");
        Assert.assertEquals(list, Lists.newArrayList(null, "doh", "kkk", "fun"));

        list.clear();
        Assert.assertEquals(list, Collections.emptyList());
        Assert.assertEquals(list.size(), 0);
        Assert.assertFalse(list.iterator().hasNext());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMapField() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <SimpleField type=\"int\" storageId=\"20\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"21\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"12\">\n"
          + "        <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"23\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));
        Transaction tx = db.createTransaction(schema, 1, true);

        ObjId id = tx.create(1);
        NavigableMap<Integer, String> map = (NavigableMap<Integer, String>)tx.readMapField(id, 12, true);
        Assert.assertEquals(map, Collections.emptyMap());
        Assert.assertEquals(Collections.emptyMap(), map);
        Assert.assertEquals(Collections.emptyMap().hashCode(), map.hashCode());
        for (Collection<?> c : Arrays.<Collection<?>>asList(map.entrySet(), map.keySet(), map.values())) {
            Assert.assertTrue(c.isEmpty());
            Assert.assertEquals(c.size(), 0);
            Assert.assertTrue(Lists.newArrayList(c.iterator()).isEmpty());
        }

        try {
            tx.readSetField(id, 12, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }
        try {
            tx.readListField(id, 12, true);
            assert false;
        } catch (UnknownFieldException e) {
            // expected
        }

    // Map

        map.put(32, "degrees");
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.isEmpty(), false);
        map.put(16, "candles");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.isEmpty(), false);
        map.put(0, null);
        Assert.assertEquals(map.size(), 3);
        Assert.assertEquals(map.isEmpty(), false);
        map.put(7, "chinese bros");
        Assert.assertEquals(map.size(), 4);
        Assert.assertEquals(map.isEmpty(), false);
        map.put(-1, "negone");
        Assert.assertEquals(map.size(), 5);
        Assert.assertEquals(map.isEmpty(), false);

        this.checkMap(map, 32, "degrees", 16, "candles", 0, null, 7, "chinese bros", -1, "negone");

        Assert.assertTrue(map.equals(map));
        Assert.assertFalse(map.equals(new Object()));

        Assert.assertTrue(map.containsKey(16));
        Assert.assertFalse(map.containsKey(1234));
        Assert.assertFalse(map.containsKey("not an integer"));

        Assert.assertTrue(map.containsValue("candles"));
        Assert.assertTrue(map.containsValue(null));
        Assert.assertTrue(map.containsValue("degrees"));
        Assert.assertFalse(map.containsValue(32));
        Assert.assertFalse(map.containsValue(-1));
        Assert.assertFalse(map.containsValue("some other value"));
        Assert.assertFalse(map.containsValue(new Object()));

        Assert.assertEquals(map.get(16), "candles");
        Assert.assertEquals(map.get(-1), "negone");
        Assert.assertEquals(map.get(0), null);
        Assert.assertEquals(map.get(12345), null);
        Assert.assertEquals(map.get(new Object()), null);

        Assert.assertTrue(map.keySet().contains(-1));
        Assert.assertTrue(map.keySet().contains(16));
        Assert.assertFalse(map.keySet().contains(1234));
        Assert.assertFalse(map.keySet().contains("candles"));
        Assert.assertFalse(map.keySet().contains(null));

        Assert.assertTrue(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(-1, "negone")));
        Assert.assertTrue(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(16, "candles")));
        Assert.assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(16, "fandles")));
        Assert.assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(-1, "negtwo")));
        Assert.assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(-1, null)));
        Assert.assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(null, null)));
        Assert.assertFalse(map.entrySet().contains(new AbstractMap.SimpleEntry<Integer, String>(null, "candles")));

        Assert.assertTrue(map.values().contains("candles"));
        Assert.assertTrue(map.values().contains(null));
        Assert.assertTrue(map.values().contains("chinese bros"));
        Assert.assertFalse(map.values().contains("japanese bros"));
        Assert.assertFalse(map.values().contains(-1));
        Assert.assertFalse(map.values().contains(new Object()));

        try {
            ((Map<Object, Object>)(Object)map).put(null, "string");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            ((Map<Object, Object>)(Object)map).put(123, 123);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            ((Map<Object, Object>)(Object)map).put("string", 54);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            ((Map<Object, Object>)(Object)map).put(54, new Object());
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        this.checkMap(map, 32, "degrees", 16, "candles", 0, null, 7, "chinese bros", -1, "negone");

        Assert.assertEquals(map.put(16, "stitches"), "candles");
        this.checkMap(map, 32, "degrees", 16, "stitches", 0, null, 7, "chinese bros", -1, "negone");

        Assert.assertEquals(map.remove(1234), null);
        this.checkMap(map, 32, "degrees", 16, "stitches", 0, null, 7, "chinese bros", -1, "negone");

        Assert.assertEquals(map.remove(7), "chinese bros");
        Assert.assertEquals(map.remove(7), null);
        this.checkMap(map, 32, "degrees", 16, "stitches", 0, null, -1, "negone");

        Assert.assertEquals(map.keySet().remove(16), true);
        Assert.assertEquals(map.keySet().remove(16), false);
        this.checkMap(map, 32, "degrees", 0, null, -1, "negone");

        Iterator<Map.Entry<Integer, String>> i1 = map.entrySet().iterator();
        Assert.assertEquals(i1.next(), new AbstractMap.SimpleEntry<Integer, String>(-1, "negone"));
        i1.remove();
        this.checkMap(map, 32, "degrees", 0, null);

        try {
            map.entrySet().add(new AbstractMap.SimpleEntry<Integer, String>(-1, "negone"));
            assert false;
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            map.keySet().add(43);
            assert false;
        } catch (UnsupportedOperationException e) {
            // expected
        }

        Iterator<Integer> i2 = map.keySet().iterator();
        Assert.assertEquals(i2.next(), (Integer)0);
        i2.remove();
        this.checkMap(map, 32, "degrees");

        ((Map<Object, Object>)(Object)map).putAll(buildMap(1, "one", 2, "two", 3, "three"));
        this.checkMap(map, 32, "degrees", 1, "one", 2, "two", 3, "three");

        try {
            map.subMap(1, 4).put(16, "stitches");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        map.subMap(1, 20).put(16, "stitches");
        this.checkMap(map, 32, "degrees", 16, "stitches", 1, "one", 2, "two", 3, "three");
        map.subMap(1, 4).remove(16);
        this.checkMap(map, 32, "degrees", 16, "stitches", 1, "one", 2, "two", 3, "three");
        Assert.assertEquals(map.subMap(1, 4).get(16), null);

    // SortedMap

        Assert.assertEquals(map.firstKey(), (Integer)1);
        Assert.assertEquals(map.firstEntry(), new AbstractMap.SimpleEntry<Integer, String>(1, "one"));

        Assert.assertEquals(map.lastKey(), (Integer)32);
        Assert.assertEquals(map.lastEntry(), new AbstractMap.SimpleEntry<Integer, String>(32, "degrees"));

        this.checkMap(map, 32, "degrees", 16, "stitches", 1, "one", 2, "two", 3, "three");

        this.checkMap((NavigableMap<Integer, String>)map.headMap(16), 1, "one", 2, "two", 3, "three");
        Assert.assertEquals(map.headMap(16), map.headMap(16, false));
        this.checkMap((NavigableMap<Integer, String>)map.headMap(17), 16, "stitches", 1, "one", 2, "two", 3, "three");
        Assert.assertEquals(map.headMap(17), map.headMap(16, true));

        this.checkMap((NavigableMap<Integer, String>)map.tailMap(3), 32, "degrees", 16, "stitches", 3, "three");
        Assert.assertEquals(map.tailMap(3), map.tailMap(3, true));
        this.checkMap((NavigableMap<Integer, String>)map.tailMap(4), 32, "degrees", 16, "stitches");
        Assert.assertEquals(map.tailMap(4), map.tailMap(3, false));

        this.checkMap((NavigableMap<Integer, String>)map.subMap(3, 16), 3, "three");
        Assert.assertEquals(map.subMap(3, 16), map.subMap(3, true, 16, false));
        this.checkMap(map.subMap(3, true, 16, true), 3, "three", 16, "stitches");
        this.checkMap(map.subMap(3, false, 16, true), 16, "stitches");
        this.checkMap(map.subMap(3, false, 16, false));

    // NavigableMap

        this.checkMap(map, 32, "degrees", 16, "stitches", 1, "one", 2, "two", 3, "three");

        NavigableMap<Integer, String> map2 = (NavigableMap<Integer, String>)map.subMap(2, 32);

        this.checkMap(map2, 16, "stitches", 2, "two", 3, "three");

        map2.navigableKeySet().subSet(3, 4).clear();
        this.checkMap(map2, 16, "stitches", 2, "two");

        map2.subMap(3, 4).put(3, "three");
        this.checkMap(map2, 16, "stitches", 2, "two", 3, "three");

        Assert.assertEquals(map2.ceilingEntry(0), new AbstractMap.SimpleEntry<Integer, String>(2, "two"));
        Assert.assertEquals(map2.ceilingEntry(2), new AbstractMap.SimpleEntry<Integer, String>(2, "two"));
        Assert.assertEquals(map2.ceilingEntry(3), new AbstractMap.SimpleEntry<Integer, String>(3, "three"));
        Assert.assertEquals(map2.ceilingEntry(4), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.ceilingEntry(16), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.ceilingEntry(17), null);
        Assert.assertEquals(map2.ceilingEntry(100), null);

        Assert.assertEquals(map2.ceilingKey(0), (Integer)2);
        Assert.assertEquals(map2.ceilingKey(3), (Integer)3);
        Assert.assertEquals(map2.ceilingKey(4), (Integer)16);
        Assert.assertEquals(map2.ceilingKey(16), (Integer)16);
        Assert.assertEquals(map2.ceilingKey(17), null);
        Assert.assertEquals(map2.ceilingKey(100), null);

        Assert.assertEquals(map2.firstEntry(), new AbstractMap.SimpleEntry<Integer, String>(2, "two"));
        Assert.assertEquals(map2.descendingMap().firstEntry(), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));

        Assert.assertEquals(map2.floorEntry(100), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.floorEntry(16), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.floorEntry(15), new AbstractMap.SimpleEntry<Integer, String>(3, "three"));
        Assert.assertEquals(map2.floorEntry(1), null);

        Assert.assertEquals(map2.floorKey(100), (Integer)16);
        Assert.assertEquals(map2.floorKey(16), (Integer)16);
        Assert.assertEquals(map2.floorKey(15), (Integer)3);
        Assert.assertEquals(map2.floorKey(1), null);

        Assert.assertEquals(map2.higherEntry(3), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.higherEntry(16), null);

        Assert.assertEquals(map2.higherKey(3), (Integer)16);
        Assert.assertEquals(map2.higherKey(16), null);

        Assert.assertEquals(map2.lastEntry(), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        Assert.assertEquals(map2.descendingMap().lastEntry(), new AbstractMap.SimpleEntry<Integer, String>(2, "two"));

        Assert.assertEquals(map2.lowerEntry(16), new AbstractMap.SimpleEntry<Integer, String>(3, "three"));
        Assert.assertEquals(map2.lowerEntry(2), null);

        Assert.assertEquals(map2.lowerKey(16), (Integer)3);
        Assert.assertEquals(map2.lowerKey(2), null);

        Assert.assertEquals(map2.navigableKeySet(), Sets.newTreeSet(Arrays.asList(2, 3, 16)));

        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(0), null);
        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(1), null);
        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(2), (Integer)1);
        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(3), (Integer)2);
        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(4), (Integer)3);
        Assert.assertEquals(map.subMap(1, true, 3, true).lowerKey(5), (Integer)3);

        Assert.assertEquals(map.subMap(2, false, 3, false).ceilingKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).ceilingKey(2), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, true).ceilingKey(2), (Integer)3);
        Assert.assertEquals(map.subMap(2, true, 3, true).ceilingKey(2), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, false).ceilingKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).ceilingKey(3), null);
        Assert.assertEquals(map.subMap(2, false, 3, true).ceilingKey(3), (Integer)3);
        Assert.assertEquals(map.subMap(2, true, 3, true).ceilingKey(3), (Integer)3);

        Assert.assertEquals(map.subMap(2, false, 3, false).higherKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).higherKey(2), null);
        Assert.assertEquals(map.subMap(2, false, 3, true).higherKey(2), (Integer)3);
        Assert.assertEquals(map.subMap(2, true, 3, true).higherKey(2), (Integer)3);
        Assert.assertEquals(map.subMap(2, false, 3, false).higherKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).higherKey(3), null);
        Assert.assertEquals(map.subMap(2, false, 3, true).higherKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, true).higherKey(3), null);

        Assert.assertEquals(map.subMap(2, false, 3, false).floorKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).floorKey(2), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, true).floorKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, true).floorKey(2), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, false).floorKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).floorKey(3), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, true).floorKey(3), (Integer)3);
        Assert.assertEquals(map.subMap(2, true, 3, true).floorKey(3), (Integer)3);

        Assert.assertEquals(map.subMap(2, false, 3, false).lowerKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).lowerKey(2), null);
        Assert.assertEquals(map.subMap(2, false, 3, true).lowerKey(2), null);
        Assert.assertEquals(map.subMap(2, true, 3, true).lowerKey(2), null);
        Assert.assertEquals(map.subMap(2, false, 3, false).lowerKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, false).lowerKey(3), (Integer)2);
        Assert.assertEquals(map.subMap(2, false, 3, true).lowerKey(3), null);
        Assert.assertEquals(map.subMap(2, true, 3, true).lowerKey(3), (Integer)2);

        for (boolean b1 : new boolean[] { false, true }) {
            for (boolean b2 : new boolean[] { false, true }) {
                final boolean empty = !b1 && !b2;
                Assert.assertEquals(map.subMap(1, b1, 3, b2).ceilingKey(0), b1 ? (Integer)1 : (Integer)2);
                Assert.assertEquals(map.subMap(1, b1, 3, b2).ceilingKey(4), null);

                Assert.assertEquals(map.subMap(1, b1, 3, b2).higherKey(0), b1 ? (Integer)1 : (Integer)2);
                Assert.assertEquals(map.subMap(1, b1, 3, b2).higherKey(4), null);

                Assert.assertEquals(map.subMap(1, b1, 3, b2).floorKey(0), null);
                Assert.assertEquals(map.subMap(1, b1, 3, b2).floorKey(4), b2 ? (Integer)3 : (Integer)2);

                Assert.assertEquals(map.subMap(1, b1, 3, b2).lowerKey(0), null);
                Assert.assertEquals(map.subMap(1, b1, 3, b2).lowerKey(4), b2 ? (Integer)3 : (Integer)2);
            }
        }

        Assert.assertEquals(map2.pollFirstEntry(), new AbstractMap.SimpleEntry<Integer, String>(2, "two"));
        this.checkMap(map, 32, "degrees", 16, "stitches", 1, "one", 3, "three");
        this.checkMap(map2, 16, "stitches", 3, "three");
        try {
            map2.put(1, "one");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        Assert.assertEquals(map2.pollLastEntry(), new AbstractMap.SimpleEntry<Integer, String>(16, "stitches"));
        this.checkMap(map, 32, "degrees", 1, "one", 3, "three");
        this.checkMap(map2, 3, "three");

        Assert.assertEquals(map.pollFirstEntry(), new AbstractMap.SimpleEntry<Integer, String>(1, "one"));
        this.checkMap(map, 32, "degrees", 3, "three");
        this.checkMap(map2, 3, "three");

        Assert.assertEquals(map2.pollFirstEntry(), new AbstractMap.SimpleEntry<Integer, String>(3, "three"));
        this.checkMap(map, 32, "degrees");
        this.checkMap(map2);

        Assert.assertEquals(map2.pollFirstEntry(), null);
        Assert.assertEquals(map2.pollLastEntry(), null);

        Assert.assertEquals(map.pollFirstEntry(), new AbstractMap.SimpleEntry<Integer, String>(32, "degrees"));
        this.checkMap(map);
        this.checkMap(map2);
    }

    private void checkMap(NavigableMap<?, ?> actual, Object... kvs) {

        final NavigableMap<Object, Object> expected = this.buildNavigableMap(kvs);

        Assert.assertEquals(actual, expected);
        Assert.assertEquals(actual.hashCode(), expected.hashCode());
        Assert.assertEquals(actual.size(), expected.size());
        Assert.assertEquals(actual.isEmpty(), expected.isEmpty());

        Assert.assertEquals(actual.descendingMap().descendingMap(), actual);
        Assert.assertEquals(actual.descendingMap().descendingMap().comparator(), actual.comparator());

        Assert.assertEquals(actual.descendingMap(), expected.descendingMap());
        Assert.assertEquals(actual.descendingMap().hashCode(), expected.descendingMap().hashCode());
        Assert.assertEquals(actual.descendingMap().size(), expected.descendingMap().size());
        Assert.assertEquals(actual.descendingMap().isEmpty(), expected.descendingMap().isEmpty());

        Assert.assertEquals(actual.keySet(), expected.keySet());
        Assert.assertEquals(actual.keySet().hashCode(), expected.keySet().hashCode());
        Assert.assertEquals(actual.keySet().size(), expected.keySet().size());
        Assert.assertEquals(actual.keySet().isEmpty(), expected.keySet().isEmpty());
        Assert.assertEquals(Lists.newArrayList(actual.keySet().iterator()), Lists.newArrayList(expected.keySet().iterator()));

        Assert.assertEquals(actual.descendingKeySet(), expected.descendingKeySet());
        Assert.assertEquals(actual.descendingKeySet().hashCode(), expected.descendingKeySet().hashCode());
        Assert.assertEquals(actual.descendingKeySet().size(), expected.descendingKeySet().size());
        Assert.assertEquals(actual.descendingKeySet().isEmpty(), expected.descendingKeySet().isEmpty());
        Assert.assertEquals(Lists.newArrayList(actual.descendingKeySet().iterator()),
          Lists.newArrayList(expected.descendingKeySet().iterator()));

        Assert.assertEquals(actual.entrySet(), expected.entrySet());
        Assert.assertEquals(actual.entrySet().hashCode(), expected.entrySet().hashCode());
        Assert.assertEquals(actual.entrySet().size(), expected.entrySet().size());
        Assert.assertEquals(actual.entrySet().isEmpty(), expected.entrySet().isEmpty());
        Assert.assertEquals(Lists.newArrayList(actual.entrySet().iterator()), Lists.newArrayList(expected.entrySet().iterator()));

        Assert.assertEquals(actual.descendingMap().entrySet(), expected.descendingMap().entrySet());
        Assert.assertEquals(actual.descendingMap().entrySet().hashCode(), expected.descendingMap().entrySet().hashCode());
        Assert.assertEquals(actual.descendingMap().entrySet().size(), expected.descendingMap().entrySet().size());
        Assert.assertEquals(actual.descendingMap().entrySet().isEmpty(), expected.descendingMap().entrySet().isEmpty());
        Assert.assertEquals(Lists.newArrayList(actual.descendingMap().entrySet().iterator()),
          Lists.newArrayList(expected.descendingMap().entrySet().iterator()));

        Assert.assertEquals(actual.values(), expected.values());
        Assert.assertEquals(actual.values().size(), expected.values().size());
        Assert.assertEquals(actual.values().isEmpty(), expected.values().isEmpty());
        Assert.assertEquals(Lists.newArrayList(actual.values().iterator()), Lists.newArrayList(expected.values().iterator()));
    }

    private NavigableMap<Object, Object> buildNavigableMap(Object... kvs) {
        final TreeMap<Object, Object> map = new TreeMap<Object, Object>();
        int i = 0;
        while (i < kvs.length - 1) {
            final Object key = kvs[i++];
            final Object value = kvs[i++];
            map.put(key, value);
        }
        return map;
    }
}

