
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IndexTest1 extends CoreAPITestSupport {

    // Params
    private final int numObjs = 25;
    private final int refMax = Math.min(numObjs, 37);
    private final Object[][] valueMatrix = new Object[][] {
        { false, true },
        { Byte.MIN_VALUE, (byte)-13, (byte)-1, (byte)0, (byte)1, (byte)37, Byte.MAX_VALUE },
        { Character.MIN_VALUE, (char)13, (char)32767, (char)32768, (char)65000, Character.MAX_VALUE },
        { Short.MIN_VALUE, (short)-8000, (short)-13, (short)-1, (short)0, (short)1, (short)13434, Short.MAX_VALUE },
        { Integer.MIN_VALUE, -40007, -8000, -13, -1, 0, 1, 13434, 41234, Integer.MAX_VALUE },
        { Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -5.343e-37f, -0.0001f, -Float.MIN_VALUE, -0.0f,
          0.0f, Float.MIN_VALUE, 1.0f, 7.3432e22f, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN },
        { Long.MIN_VALUE, -84758724343392L, -40007L, -8000L, -13L, -1L, 0L, 1L, 13434L, 41234L, 875744654299L, Long.MAX_VALUE },
        { Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -3.299e-99, -5.343e-63, -0.0001, -Double.MIN_VALUE, -0.0,
          0.0, Double.MIN_VALUE, 1.0, 7.3432e45, 2.48754e99, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN },
        { "", "\u0000", "abc", "\u0001", "\u00005\u0001\u0000", "\u0000\u0001",
          "abc\u0000", "abcd", "fgh", "\u5678blah", "\ud9ff", "\uffff", "\uffff\uffff\uffff", null },
    };

    @Test
    public void testSimpleFieldIndexes() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"z\" encoding=\"urn:fdc:permazen.io:2020:boolean\" storageId=\"10\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"b\" encoding=\"urn:fdc:permazen.io:2020:byte\" storageId=\"11\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"c\" encoding=\"urn:fdc:permazen.io:2020:char\" storageId=\"12\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:short\" storageId=\"13\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"14\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"f\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"15\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"j\" encoding=\"urn:fdc:permazen.io:2020:long\" storageId=\"16\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"d\" encoding=\"urn:fdc:permazen.io:2020:double\" storageId=\"17\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"str\" encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"18\" indexed=\"true\"/>\n"
          + "    <ReferenceField name=\"r\" storageId=\"19\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        // Create objects
        Transaction tx = db.createTransaction(schema1, 1, true);
        final ObjId[] ids = new ObjId[numObjs];
        for (int i = 0; i < ids.length; i++)
            ids[i] = tx.create(1);
        //this.showKV(tx, "testSimpleFieldIndexes: 1");

        // Build mapping of references
        final TreeMap<ObjId, ObjId> refMap = new TreeMap<>();
        for (ObjId id : ids) {
            final int r = this.random.nextInt(refMax + 1);
            refMap.put(id, r < refMax ? ids[r] : null);
        }

        // Set object fields
        for (int i = 0; i < ids.length; i++) {
            for (int j = 0; j < this.valueMatrix.length; j++) {
                final Object[] fieldValues = this.valueMatrix[j];
                tx.writeSimpleField(ids[i], 10 + j, fieldValues[i % fieldValues.length], true);
            }
            final int r = this.random.nextInt(refMax + 1);
            tx.writeSimpleField(ids[i], 19, refMap.get(ids[i]), true);
        }
        //this.showKV(tx, "testSimpleFieldIndexes: 2");

        // Verify non-reference indexes - objects
        for (int i = 0; i < this.valueMatrix.length; i++) {
            final Object[] fieldValues = this.valueMatrix[i];
            for (int j = 0; j < fieldValues.length; j++) {
                final Object value = fieldValues[j];

                // Build set of objects that should have value #j in field #i
                final TreeSet<ObjId> expected = new TreeSet<>();
                for (int k = j; k < ids.length; k += fieldValues.length)
                    expected.add(ids[k]);

                // Query index
                //this.log.info("testSimpleFieldIndexes: checking for value `{}' in field #{}", value, 10 + i);
                Assert.assertEquals(tx.queryIndex(10 + i).asMap().get(value), expected);
            }
        }

        // Verify reference index - objects
        for (int i = 0; i <= refMax; i++) {

            // Build set of objects referring to object #i
            final ObjId ref = i < refMax ? ids[i] : null;
            final TreeSet<ObjId> expected = new TreeSet<>();
            for (ObjId id : ids) {
                final ObjId jref = refMap.get(id);
                if (ref != null ? ref.equals(jref) : jref == null)
                    expected.add(id);
            }

            // Query index
            //this.log.info("testSimpleFieldIndexes: checking for reference {} in field #19", ref);
            Assert.assertEquals(tx.queryIndex(19).asMap().get(ref), !expected.isEmpty() ? expected : null);
        }

        //this.showKV(tx, "testSimpleFieldIndexes: 3");

        // Verify non-reference indexes - values
        for (int i = 0; i < this.valueMatrix.length; i++)
            this.verifyValues(tx, 10 + i, this.valueMatrix[i][0].getClass(), numObjs, this.valueMatrix[i]);

        // Verify reference index - values
        this.verifyValues(tx, 19, ObjId.class, numObjs, new HashSet<>(refMap.values()).toArray());

        tx.commit();
    }

    @Test
    public void testArrayFieldIndexes() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"z\" encoding=\"urn:fdc:permazen.io:2020:boolean[]\" storageId=\"10\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"b\" encoding=\"urn:fdc:permazen.io:2020:byte[]\" storageId=\"11\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"c\" encoding=\"urn:fdc:permazen.io:2020:char[]\" storageId=\"12\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:short[]\" storageId=\"13\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int[]\" storageId=\"14\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"f\" encoding=\"urn:fdc:permazen.io:2020:float[]\" storageId=\"15\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"j\" encoding=\"urn:fdc:permazen.io:2020:long[]\" storageId=\"16\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"d\" encoding=\"urn:fdc:permazen.io:2020:double[]\" storageId=\"17\" indexed=\"true\"/>\n"
          + "    <SimpleField name=\"str\" encoding=\"urn:fdc:permazen.io:2020:String[]\" storageId=\"18\" indexed=\"true\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Class<?>[] elementTypes = new Class<?>[] {
            boolean.class,
            byte.class,
            char.class,
            short.class,
            int.class,
            float.class,
            long.class,
            double.class,
            String.class,
        };

        // Create objects
        Transaction tx = db.createTransaction(schema1, 1, true);
        final ObjId[] ids = new ObjId[numObjs];
        for (int i = 0; i < ids.length; i++)
            ids[i] = tx.create(1);
        //this.showKV(tx, "testArrayFieldIndexes: 1");

        // Set object fields
        for (int i = 0; i < ids.length; i++) {
            for (int j = 0; j < this.valueMatrix.length; j++) {
                final Class<?> elementType = elementTypes[j];
                final Object[] fieldValues = this.valueMatrix[j];
                final Object array = Array.newInstance(elementType, 1);
                Array.set(array, 0, fieldValues[i % fieldValues.length]);
                tx.writeSimpleField(ids[i], 10 + j, array, true);
            }
        }
        //this.showKV(tx, "testArrayFieldIndexes: 2");

        // Verify non-reference indexes - objects
        for (int i = 0; i < this.valueMatrix.length; i++) {
            final Class<?> elementType = elementTypes[i];
            final Object[] fieldValues = this.valueMatrix[i];
            for (int j = 0; j < fieldValues.length; j++) {
                final Object value = fieldValues[j];
                final Object array = Array.newInstance(elementType, 1);
                Array.set(array, 0, value);

                // Build set of objects that should have array { value #j } in field #i
                final TreeSet<ObjId> expected = new TreeSet<>();
                for (int k = j; k < ids.length; k += fieldValues.length)
                    expected.add(ids[k]);

                // Query index
                //this.log.info("testArrayFieldIndexes: checking for array with `{}' in field #{}", value, 10 + i);
                Assert.assertEquals(tx.queryIndex(10 + i).asMap().get(array), expected);
            }
        }

        //this.showKV(tx, "testArrayFieldIndexes: 3");

        tx.commit();
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> void verifyValues(Transaction tx, int storageId, Class<?> type0, int max, Object[] values) {
        final FieldType<T> fieldType = ((SimpleField<T>)tx.getSchema().getObjType(1).getField(storageId)).getFieldType();
        final Class<T> type = (Class<T>)type0;
        final ArrayList<T> actual = new ArrayList<>((NavigableSet<T>)tx.queryIndex(storageId).asMap().navigableKeySet());
        final ArrayList<T> sorted = new ArrayList<>(actual);
        Collections.sort(sorted, fieldType);
        Assert.assertEquals(actual, sorted, "actual = " + actual + ", sorted = " + sorted);
        final ArrayList<T> expected = new ArrayList<>();
        int count = 0;
        for (Object value : values) {
            if (count++ >= max)
                break;
            expected.add(type.cast(value));
        }
        Collections.sort(expected, fieldType);
        Assert.assertEquals(actual, expected, "actual = " + actual + ", expected = " + expected);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexFieldIndexes() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SetField name=\"set\" storageId=\"10\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"20\" indexed=\"true\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"11\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"21\" indexed=\"true\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"12\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\" indexed=\"true\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"23\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        Transaction tx = db.createTransaction(schema, 1, true);

        ObjId[] ids = new ObjId[] { null, tx.create(1), tx.create(1), tx.create(1) };

        for (int i = 1; i <= 3; i++) {
            final ObjId id = ids[i];

            NavigableSet<Integer> set = (NavigableSet<Integer>)tx.readSetField(id, 10, true);
            set.add(100 + i);
            set.add(200 + i);
            set.add(300);

            List<String> list = (List<String>)tx.readListField(id, 11, true);
            list.add("foo" + i);
            list.add("bar" + i);
            list.add("jam");

            NavigableMap<Integer, String> map = (NavigableMap<Integer, String>)tx.readMapField(id, 12, true);
            map.put(1000 + i, "valueA" + i);
            map.put(2000 + i, "valueB");
            map.put(3000, "valueC" + i);
            map.put(4000, "valueD");
        }

    // Sets

        Assert.assertEquals(tx.queryIndex(20).asMap().get(999), null);
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(101), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(102), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(103), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(201), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(202), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(203), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(20).asMap().get(300), buildSet(ids[1], ids[2], ids[3]));

        TestSupport.checkMap(tx.queryIndex(20).asMap(), buildSortedMap(
          101,  buildSortedSet(ids[1]),
          102,  buildSortedSet(ids[2]),
          103,  buildSortedSet(ids[3]),
          201,  buildSortedSet(ids[1]),
          202,  buildSortedSet(ids[2]),
          203,  buildSortedSet(ids[3]),
          300,  buildSortedSet(ids[1], ids[2], ids[3])));

    // Lists

        Assert.assertEquals(tx.queryIndex(21).asMap().get("blah"), null);
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("foo1"), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("foo2"), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("foo3"), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("bar1"), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("bar2"), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("bar3"), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(21).asMap().get("jam"), buildSet(ids[1], ids[2], ids[3]));

        TestSupport.checkMap(tx.queryIndex(21).asMap(), buildSortedMap(
          "foo1",   buildSortedSet(ids[1]),
          "foo2",   buildSortedSet(ids[2]),
          "foo3",   buildSortedSet(ids[3]),
          "bar1",   buildSortedSet(ids[1]),
          "bar2",   buildSortedSet(ids[2]),
          "bar3",   buildSortedSet(ids[3]),
          "jam",    buildSortedSet(ids[1], ids[2], ids[3])));

        Assert.assertEquals(tx.queryListElementIndex(21).asMapOfIndex().get("blah"), null);
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("foo1").asMap(), buildMap(ids[1], buildSet(0)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("foo2").asMap(), buildMap(ids[2], buildSet(0)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("foo3").asMap(), buildMap(ids[3], buildSet(0)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("bar1").asMap(), buildMap(ids[1], buildSet(1)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("bar2").asMap(), buildMap(ids[2], buildSet(1)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("bar3").asMap(), buildMap(ids[3], buildSet(1)));
        TestSupport.checkMap(tx.queryListElementIndex(21).asMapOfIndex().get("jam").asMap(),  buildMap(
          ids[1], buildSet(2),
          ids[2], buildSet(2),
          ids[3], buildSet(2)));

        TestSupport.checkSet(tx.queryListElementIndex(21).asSet(), buildSet(
          new Tuple3<>("foo1", ids[1], 0),
          new Tuple3<>("foo2", ids[2], 0),
          new Tuple3<>("foo3", ids[3], 0),
          new Tuple3<>("bar1", ids[1], 1),
          new Tuple3<>("bar2", ids[2], 1),
          new Tuple3<>("bar3", ids[3], 1),
          new Tuple3<>("jam", ids[1], 2),
          new Tuple3<>("jam", ids[2], 2),
          new Tuple3<>("jam", ids[3], 2)));

    // Map Keys

        Assert.assertEquals(tx.queryIndex(22).asMap().get(999), null);
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1001), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1002), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1003), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2001), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2002), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2003), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(3000), buildSet(ids[1], ids[2], ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(4000), buildSet(ids[1], ids[2], ids[3]));

        TestSupport.checkMap(tx.queryIndex(22).asMap(), buildSortedMap(
          1001,     buildSortedSet(ids[1]),
          1002,     buildSortedSet(ids[2]),
          1003,     buildSortedSet(ids[3]),
          2001,     buildSortedSet(ids[1]),
          2002,     buildSortedSet(ids[2]),
          2003,     buildSortedSet(ids[3]),
          3000,     buildSortedSet(ids[1], ids[2], ids[3]),
          4000,     buildSortedSet(ids[1], ids[2], ids[3])));

        Assert.assertEquals(tx.queryIndex(22).asMap().get(999), null);
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1001),
          buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1002),
          buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(1003),
          buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2001),
          buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2002),
          buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(2003),
          buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(3000), buildSet(ids[1], ids[2], ids[3]));
        TestSupport.checkSet(tx.queryIndex(22).asMap().get(4000), buildSet(ids[1], ids[2], ids[3]));

        TestSupport.checkMap(tx.queryIndex(22).asMap(), buildSortedMap(
          1001,     buildSet(ids[1]),
          1002,     buildSet(ids[2]),
          1003,     buildSet(ids[3]),
          2001,     buildSet(ids[1]),
          2002,     buildSet(ids[2]),
          2003,     buildSet(ids[3]),
          3000,     buildSet(ids[1], ids[2], ids[3]),
          4000,     buildSet(ids[1], ids[2], ids[3])));

    // Map Values

        Assert.assertEquals(tx.queryIndex(23).asMap().get("blah"), null);
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueA1"), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueA2"), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueA3"), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueB"), buildSet(ids[1], ids[2], ids[3]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueC1"), buildSet(ids[1]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueC2"), buildSet(ids[2]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueC3"), buildSet(ids[3]));
        TestSupport.checkSet(tx.queryIndex(23).asMap().get("valueD"), buildSet(ids[1], ids[2], ids[3]));

        TestSupport.checkMap(tx.queryIndex(23).asMap(), buildSortedMap(
          "valueA1",    buildSortedSet(ids[1]),
          "valueA2",    buildSortedSet(ids[2]),
          "valueA3",    buildSortedSet(ids[3]),
          "valueB",     buildSortedSet(ids[1], ids[2], ids[3]),
          "valueC1",    buildSortedSet(ids[1]),
          "valueC2",    buildSortedSet(ids[2]),
          "valueC3",    buildSortedSet(ids[3]),
          "valueD",     buildSortedSet(ids[1], ids[2], ids[3])));

        Assert.assertEquals(tx.queryMapValueIndex(23).asMapOfIndex().get("blah"), null);
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueA1").asSet(),
          buildSet(new Tuple2<>(ids[1], 1001)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueA2").asSet(),
          buildSet(new Tuple2<>(ids[2], 1002)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueA3").asSet(),
          buildSet(new Tuple2<>(ids[3], 1003)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueB").asSet(), buildSet(
          new Tuple2<>(ids[1], 2001),
          new Tuple2<>(ids[2], 2002),
          new Tuple2<>(ids[3], 2003)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueC1").asSet(),
          buildSet(new Tuple2<>(ids[1], 3000)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueC2").asSet(),
          buildSet(new Tuple2<>(ids[2], 3000)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueC3").asSet(),
          buildSet(new Tuple2<>(ids[3], 3000)));
        TestSupport.checkSet(tx.queryMapValueIndex(23).asMapOfIndex().get("valueD").asSet(), buildSet(
          new Tuple2<>(ids[1], 4000),
          new Tuple2<>(ids[2], 4000),
          new Tuple2<>(ids[3], 4000)));

        TestSupport.checkSet(tx.queryMapValueIndex(23).asSet(), buildSet(
          new Tuple3<>("valueA1", ids[1], 1001),
          new Tuple3<>("valueA2", ids[2], 1002),
          new Tuple3<>("valueA3", ids[3], 1003),
          new Tuple3<>("valueB", ids[1], 2001),
          new Tuple3<>("valueB", ids[2], 2002),
          new Tuple3<>("valueB", ids[3], 2003),
          new Tuple3<>("valueC1", ids[1], 3000),
          new Tuple3<>("valueC2", ids[2], 3000),
          new Tuple3<>("valueC3", ids[3], 3000),
          new Tuple3<>("valueD", ids[1], 4000),
          new Tuple3<>("valueD", ids[2], 4000),
          new Tuple3<>("valueD", ids[3], 4000)));

        TestSupport.checkMap(tx.queryMapValueIndex(23).asMap(), buildMap(
          new Tuple2<>("valueA1", ids[1]), buildSet(1001),
          new Tuple2<>("valueA2", ids[2]), buildSet(1002),
          new Tuple2<>("valueA3", ids[3]), buildSet(1003),
          new Tuple2<>("valueB", ids[1]), buildSet(2001),
          new Tuple2<>("valueB", ids[2]), buildSet(2002),
          new Tuple2<>("valueB", ids[3]), buildSet(2003),
          new Tuple2<>("valueC1", ids[1]), buildSet(3000),
          new Tuple2<>("valueC2", ids[2]), buildSet(3000),
          new Tuple2<>("valueC3", ids[3]), buildSet(3000),
          new Tuple2<>("valueD", ids[1]), buildSet(4000),
          new Tuple2<>("valueD", ids[2]), buildSet(4000),
          new Tuple2<>("valueD", ids[3]), buildSet(4000)));

        TestSupport.checkMap(tx.queryMapValueIndex(23).asIndex().asMap(), buildSortedMap(
          "valueA1",    buildSet(ids[1]),
          "valueA2",    buildSet(ids[2]),
          "valueA3",    buildSet(ids[3]),
          "valueB",     buildSet(ids[1], ids[2], ids[3]),
          "valueC1",    buildSet(ids[1]),
          "valueC2",    buildSet(ids[2]),
          "valueC3",    buildSet(ids[3]),
          "valueD",     buildSet(ids[1], ids[2], ids[3])));

    // Values

        TestSupport.checkSet(tx.queryIndex(20).asMap().navigableKeySet(),
          buildSortedSet(101, 102, 103, 201, 202, 203, 300));
        TestSupport.checkSet(tx.queryIndex(21).asMap().navigableKeySet(),
          buildSortedSet("foo1", "foo2", "foo3", "bar1", "bar2", "bar3", "jam"));
        TestSupport.checkSet(tx.queryIndex(22).asMap().navigableKeySet(),
          buildSortedSet(1001, 1002, 1003, 2001, 2002, 2003, 3000, 4000));
        TestSupport.checkSet(tx.queryIndex(23).asMap().navigableKeySet(),
          buildSortedSet("valueA1", "valueA2", "valueA3", "valueB", "valueC1", "valueC2", "valueC3", "valueD"));
    }
}

