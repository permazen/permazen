
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.testng.Assert;
import org.testng.annotations.Test;

// This is to test what happens when the same reference field storage ID is used in different
// object types with different referenced Java types for the reference field and then we
// query the indexes on those fields.
public class DuplicateReferenceFieldTest extends TestSupport {

    @Test
    public void testDuplicateReferenceFields() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(ClassA.class, ClassB.class, ClassC.class);

        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final ClassA a1 = jtx.create(ClassA.class);
            final ClassA a2 = jtx.create(ClassA.class);

            final ClassB b1 = jtx.create(ClassB.class);
            final ClassB b2 = jtx.create(ClassB.class);

            final ClassC c1 = jtx.create(ClassC.class);
            final ClassC c2 = jtx.create(ClassC.class);

            // Name
            a1.setName("foo");
            b1.setName("foo");
            c1.setName("foo");

            a2.setName("bar-a");
            b2.setName("bar-b");
            c2.setName("bar-c");

            // FieldR
            a1.setFieldR(b1);
            a2.setFieldR(a1);
            b1.setFieldR(c1);
            b2.setFieldR(b2);
            c1.setFieldR(a1);
            c2.setFieldR(a2);

            // FieldX
            b1.getFieldX().add(2.0f);
            b1.getFieldX().add(3.0f);
            b1.getFieldX().add(4.0f);

            c1.getFieldX().add(3.0f);
            c1.getFieldX().add(4.0f);
            c1.getFieldX().add(5.0f);

            // FieldY
            a1.getFieldY().add(100);
            a1.getFieldY().add(200);
            a1.getFieldY().add(200);
            a1.getFieldY().add(200);
            a1.getFieldY().add(300);

            c1.getFieldY().add(300);
            c1.getFieldY().add(400);
            c1.getFieldY().add(400);
            c1.getFieldY().add(400);
            c1.getFieldY().add(500);

            // FieldZ
            a1.getFieldZ().put(a2, "a2");
            a1.getFieldZ().put(b2, "b2");
            a1.getFieldZ().put(c2, "c2");

            b1.getFieldZ().put(c1, "c1");
            b1.getFieldZ().put(c2, "c2");

        // Check index query value types

            // FieldR
            final HashMap<Class<?>, Class<?>> valueTypeMap = new HashMap<>();
            valueTypeMap.put(Object.class, JObject.class);
            valueTypeMap.put(JObject.class, JObject.class);
            valueTypeMap.put(TopClass.class, JObject.class);
            valueTypeMap.put(ClassA.class, JObject.class);
            valueTypeMap.put(ClassB.class, TopClass.class);
            valueTypeMap.put(ClassC.class, ClassA.class);
            for (Class<?> startType : valueTypeMap.keySet()) {
                for (Class<?> valueType : valueTypeMap.keySet())
                    this.verifyValueType(jtx, startType, "fieldR", valueType, valueTypeMap.get(startType));
            }

            // FieldZ.key
            valueTypeMap.clear();
            valueTypeMap.put(Object.class, TopClass.class);
            valueTypeMap.put(JObject.class, TopClass.class);
            valueTypeMap.put(TopClass.class, TopClass.class);
            valueTypeMap.put(ClassA.class, TopClass.class);
            valueTypeMap.put(ClassB.class, ClassC.class);
            for (Class<?> startType : valueTypeMap.keySet()) {
                for (Class<?> valueType : valueTypeMap.keySet())
                    this.verifyValueType(jtx, startType, "fieldZ.key", valueType, valueTypeMap.get(startType));
            }

        // Verify indexes

            // Name
            Assert.assertEquals(jtx.queryIndex(TopClass.class, "name", String.class), buildMap(
              "foo", buildSet(a1, b1, c1),
              "bar-a", buildSet(a2),
              "bar-b", buildSet(b2),
              "bar-c", buildSet(c2)));
            Assert.assertEquals(jtx.queryIndex(ClassA.class, "name", String.class), buildMap(
              "foo", buildSet(a1),
              "bar-a", buildSet(a2)));
            Assert.assertEquals(jtx.queryIndex(ClassB.class, "name", String.class), buildMap(
              "foo", buildSet(b1),
              "bar-b", buildSet(b2)));
            Assert.assertEquals(jtx.queryIndex(ClassC.class, "name", String.class), buildMap(
              "foo", buildSet(c1),
              "bar-c", buildSet(c2)));

            // FieldR - check indexes
            Assert.assertEquals(jtx.queryIndex(TopClass.class, "fieldR", JObject.class), buildMap(
              a1, buildSet(a2, c1),
              a2, buildSet(c2),
              b1, buildSet(a1),
              b2, buildSet(b2),
              c1, buildSet(b1)));
            Assert.assertEquals(jtx.queryIndex(ClassA.class, "fieldR", JObject.class), buildMap(
              a1, buildSet(a2),
              b1, buildSet(a1)));
            Assert.assertEquals(jtx.queryIndex(ClassB.class, "fieldR", TopClass.class), buildMap(
              c1, buildSet(b1),
              b2, buildSet(b2)));
            Assert.assertEquals(jtx.queryIndex(ClassC.class, "fieldR", ClassA.class), buildMap(
              a1, buildSet(c1),
              a2, buildSet(c2)));

            // FieldX
            Assert.assertEquals(jtx.queryIndex(TopClass.class, "fieldX.element", Float.class), buildMap(
              2.0f, buildSet(b1),
              3.0f, buildSet(b1, c1),
              4.0f, buildSet(b1, c1),
              5.0f, buildSet(c1)));
            Assert.assertEquals(jtx.queryIndex(ClassB.class, "fieldX.element", Float.class), buildMap(
              2.0f, buildSet(b1),
              3.0f, buildSet(b1),
              4.0f, buildSet(b1)));
            Assert.assertEquals(jtx.queryIndex(ClassC.class, "fieldX.element", Float.class), buildMap(
              3.0f, buildSet(c1),
              4.0f, buildSet(c1),
              5.0f, buildSet(c1)));

            // FieldY
            Assert.assertEquals(jtx.queryIndex(TopClass.class, "fieldY.element", Integer.class), buildMap(
              100, buildSet(a1),
              200, buildSet(a1),
              300, buildSet(a1, c1),
              400, buildSet(c1),
              500, buildSet(c1)));
            Assert.assertEquals(jtx.queryIndex(ClassA.class, "fieldY.element", Integer.class), buildMap(
              100, buildSet(a1),
              200, buildSet(a1),
              300, buildSet(a1)));
            Assert.assertEquals(jtx.queryIndex(ClassC.class, "fieldY.element", Integer.class), buildMap(
              300, buildSet(c1),
              400, buildSet(c1),
              500, buildSet(c1)));

            // FieldZ
            Assert.assertEquals(jtx.queryIndex(TopClass.class, "fieldZ.key", TopClass.class), buildMap(
              a2, buildSet(a1),
              b2, buildSet(a1),
              c2, buildSet(a1, b1),
              c1, buildSet(b1)));
            Assert.assertEquals(jtx.queryIndex(ClassA.class, "fieldZ.key", TopClass.class), buildMap(
              a2, buildSet(a1),
              b2, buildSet(a1),
              c2, buildSet(a1)));           // note b2 does not appear in set
            Assert.assertEquals(jtx.queryIndex(ClassB.class, "fieldZ.key", ClassC.class), buildMap(
              c1, buildSet(b1),
              c2, buildSet(b1)));           // note a1 does not appear in set

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private void verifyValueType(JTransaction jtx, Class<?> startType, String fieldName, Class<?> valueType, Class<?> correctType) {
        IllegalArgumentException failure = null;
        try {
            jtx.queryIndex(startType, fieldName, valueType);
        } catch (IllegalArgumentException e) {
            failure = e;
        }
        assert (failure != null) == (valueType != correctType) : "unexpected " + (failure != null ? "failure" : "success")
          + " for index query with start type " + startType + ", field `" + fieldName + "', and value type " + valueType
          + "; correct value type is " + correctType + "; failure = " + failure;
    }

// Model Classes

    public abstract static class TopClass implements JObject {

        @JField(storageId = 101, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @Override
        public String toString() {
            return this.getClass().getSimpleName().replaceAll("^.*\\$([^$]+)\\$.*$", "$1") + "@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 200)
    public abstract static class ClassA extends TopClass {

        // Field R
        @JField(storageId = 120)
        public abstract JObject getFieldR();                            // note reference type is JObject
        public abstract void setFieldR(JObject ref);

        // Field Y
        @JListField(storageId = 310, element = @JField(storageId = 311, type = "int", indexed = true))
        public abstract List<Integer> getFieldY();

        // Field Z
        @JMapField(storageId = 320,
          key = @JField(storageId = 321, indexed = true),
          value = @JField(storageId = 322, indexed = true))
        public abstract NavigableMap<TopClass, String> getFieldZ();     // note key type is TopClass
    }

    @JSimpleClass(storageId = 210)
    public abstract static class ClassB extends TopClass {

        // Field R
        @JField(storageId = 120)
        public abstract TopClass getFieldR();                           // note reference type is TopClass
        public abstract void setFieldR(TopClass ref);

        // Field X
        @JSetField(storageId = 300, element = @JField(storageId = 301, type = "float", indexed = true))
        public abstract NavigableSet<Float> getFieldX();

        // Field Z
        @JMapField(storageId = 320,
          key = @JField(storageId = 321, indexed = true),
          value = @JField(storageId = 322, indexed = true))
        public abstract NavigableMap<ClassC, String> getFieldZ();       // note key type is ClassC
    }

    @JSimpleClass(storageId = 220)
    public abstract static class ClassC extends TopClass {

        // Field R
        @JField(storageId = 120)
        public abstract ClassA getFieldR();                             // note reference type is ClassA
        public abstract void setFieldR(ClassA ref);

        // Field X
        @JSetField(storageId = 300, element = @JField(storageId = 301, type = "float", indexed = true))
        public abstract NavigableSet<Float> getFieldX();

        // Field C
        @JListField(storageId = 310, element = @JField(storageId = 311, type = "int", indexed = true))
        public abstract List<Integer> getFieldY();
    }
}

