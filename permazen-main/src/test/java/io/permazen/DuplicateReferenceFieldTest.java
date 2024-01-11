
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.testng.annotations.Test;

// This is to test what happens when the same reference field storage ID is used in different
// object types with different referenced Java types for the reference field and then we
// query the indexes on those fields.
public class DuplicateReferenceFieldTest extends MainTestSupport {

    @Test
    public void testDuplicateReferenceFields() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(ClassA.class, ClassB.class, ClassC.class);

        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            final ClassA a1 = ptx.create(ClassA.class);
            final ClassA a2 = ptx.create(ClassA.class);

            final ClassB b1 = ptx.create(ClassB.class);
            final ClassB b2 = ptx.create(ClassB.class);

            final ClassC c1 = ptx.create(ClassC.class);
            final ClassC c2 = ptx.create(ClassC.class);

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
            valueTypeMap.put(Object.class, PermazenObject.class);
            valueTypeMap.put(PermazenObject.class, PermazenObject.class);
            valueTypeMap.put(TopClass.class, PermazenObject.class);
            valueTypeMap.put(ClassA.class, PermazenObject.class);
            valueTypeMap.put(ClassB.class, TopClass.class);
            valueTypeMap.put(ClassC.class, ClassA.class);
            for (Class<?> startType : valueTypeMap.keySet()) {
                for (Class<?> valueType : valueTypeMap.keySet())
                    this.verifyValueType(ptx, startType, "fieldR", valueType, valueTypeMap.get(startType));
            }

            // FieldZ.key
            valueTypeMap.clear();
            valueTypeMap.put(Object.class, TopClass.class);
            valueTypeMap.put(PermazenObject.class, TopClass.class);
            valueTypeMap.put(TopClass.class, TopClass.class);
            valueTypeMap.put(ClassA.class, TopClass.class);
            valueTypeMap.put(ClassB.class, ClassC.class);
            for (Class<?> startType : valueTypeMap.keySet()) {
                for (Class<?> valueType : valueTypeMap.keySet())
                    this.verifyValueType(ptx, startType, "fieldZ.key", valueType, valueTypeMap.get(startType));
            }

        // Verify indexes

            // Name
            TestSupport.checkMap(ptx.querySimpleIndex(TopClass.class, "name", String.class).asMap(), buildMap(
              "foo", buildSet(a1, b1, c1),
              "bar-a", buildSet(a2),
              "bar-b", buildSet(b2),
              "bar-c", buildSet(c2)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassA.class, "name", String.class).asMap(), buildMap(
              "foo", buildSet(a1),
              "bar-a", buildSet(a2)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassB.class, "name", String.class).asMap(), buildMap(
              "foo", buildSet(b1),
              "bar-b", buildSet(b2)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassC.class, "name", String.class).asMap(), buildMap(
              "foo", buildSet(c1),
              "bar-c", buildSet(c2)));

            // FieldR - check indexes
            TestSupport.checkMap(ptx.querySimpleIndex(TopClass.class, "fieldR", PermazenObject.class).asMap(), buildMap(
              a1, buildSet(a2, c1),
              a2, buildSet(c2),
              b1, buildSet(a1),
              b2, buildSet(b2),
              c1, buildSet(b1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassA.class, "fieldR", PermazenObject.class).asMap(), buildMap(
              a1, buildSet(a2),
              b1, buildSet(a1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassB.class, "fieldR", TopClass.class).asMap(), buildMap(
              c1, buildSet(b1),
              b2, buildSet(b2)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassC.class, "fieldR", ClassA.class).asMap(), buildMap(
              a1, buildSet(c1),
              a2, buildSet(c2)));

            // FieldX
            TestSupport.checkMap(ptx.querySimpleIndex(TopClass.class, "fieldX.element", Float.class).asMap(), buildMap(
              2.0f, buildSet(b1),
              3.0f, buildSet(b1, c1),
              4.0f, buildSet(b1, c1),
              5.0f, buildSet(c1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassB.class, "fieldX.element", Float.class).asMap(), buildMap(
              2.0f, buildSet(b1),
              3.0f, buildSet(b1),
              4.0f, buildSet(b1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassC.class, "fieldX.element", Float.class).asMap(), buildMap(
              3.0f, buildSet(c1),
              4.0f, buildSet(c1),
              5.0f, buildSet(c1)));

            // FieldY
            TestSupport.checkMap(ptx.querySimpleIndex(TopClass.class, "fieldY.element", Integer.class).asMap(), buildMap(
              100, buildSet(a1),
              200, buildSet(a1),
              300, buildSet(a1, c1),
              400, buildSet(c1),
              500, buildSet(c1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassA.class, "fieldY.element", Integer.class).asMap(), buildMap(
              100, buildSet(a1),
              200, buildSet(a1),
              300, buildSet(a1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassC.class, "fieldY.element", Integer.class).asMap(), buildMap(
              300, buildSet(c1),
              400, buildSet(c1),
              500, buildSet(c1)));

            // FieldZ
            TestSupport.checkMap(ptx.querySimpleIndex(TopClass.class, "fieldZ.key", TopClass.class).asMap(), buildMap(
              a2, buildSet(a1),
              b2, buildSet(a1),
              c2, buildSet(a1, b1),
              c1, buildSet(b1)));
            TestSupport.checkMap(ptx.querySimpleIndex(ClassA.class, "fieldZ.key", TopClass.class).asMap(), buildMap(
              a2, buildSet(a1),
              b2, buildSet(a1),
              c2, buildSet(a1)));           // note b2 does not appear in set
            TestSupport.checkMap(ptx.querySimpleIndex(ClassB.class, "fieldZ.key", ClassC.class).asMap(), buildMap(
              c1, buildSet(b1),
              c2, buildSet(b1)));           // note a1 does not appear in set

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private void verifyValueType(PermazenTransaction ptx,
      Class<?> startType, String fieldName, Class<?> valueType, Class<?> correctType) {
        IllegalArgumentException failure = null;
        try {
            ptx.querySimpleIndex(startType, fieldName, valueType);
        } catch (IllegalArgumentException e) {
            failure = e;
        }
        boolean comparable = valueType.isAssignableFrom(correctType) || correctType.isAssignableFrom(valueType);
        assert (failure != null) == !comparable : "unexpected " + (failure != null ? "failure" : "success")
          + " for index query with start type " + startType + ", field \"" + fieldName + "\", and value type " + valueType
          + "; comparable value type is " + correctType + "; failure = " + failure;
    }

// Model Classes

    public abstract static class TopClass implements PermazenObject {

        @PermazenField(indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @Override
        public String toString() {
            return this.getClass().getSimpleName().replaceAll("^.*\\$([^$]+)\\$.*$", "$1") + "@" + this.getObjId();
        }
    }

    @PermazenType
    public abstract static class ClassA extends TopClass {

        // Field R
        public abstract PermazenObject getFieldR();                            // note reference type is PermazenObject
        public abstract void setFieldR(PermazenObject ref);

        // Field Y
        @PermazenListField(element = @PermazenField(encoding = "int", indexed = true))
        public abstract List<Integer> getFieldY();

        // Field Z
        @PermazenMapField(key = @PermazenField(indexed = true), value = @PermazenField(indexed = true))
        public abstract NavigableMap<TopClass, String> getFieldZ();     // note key type is TopClass
    }

    @PermazenType
    public abstract static class ClassB extends TopClass {

        // Field R
        public abstract TopClass getFieldR();                           // note reference type is TopClass
        public abstract void setFieldR(TopClass ref);

        // Field X
        @PermazenSetField(element = @PermazenField(encoding = "float", indexed = true))
        public abstract NavigableSet<Float> getFieldX();

        // Field Z
        @PermazenMapField(key = @PermazenField(indexed = true), value = @PermazenField(indexed = true))
        public abstract NavigableMap<ClassC, String> getFieldZ();       // note key type is ClassC
    }

    @PermazenType
    public abstract static class ClassC extends TopClass {

        // Field R
        public abstract ClassA getFieldR();                             // note reference type is ClassA
        public abstract void setFieldR(ClassA ref);

        // Field X
        @PermazenSetField(element = @PermazenField(encoding = "float", indexed = true))
        public abstract NavigableSet<Float> getFieldX();

        // Field C
        @PermazenListField(element = @PermazenField(encoding = "int", indexed = true))
        public abstract List<Integer> getFieldY();
    }
}
