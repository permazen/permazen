
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.annotation.OnChange;
import io.permazen.change.FieldChange;
import io.permazen.change.ListFieldChange;
import io.permazen.change.SetFieldChange;
import io.permazen.change.SimpleFieldChange;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * This test just proves that fields with the same name can have different types in different
 * classes as long as they are not both indexed.
 */
public class FieldTypesConflictTest extends TestSupport {

    @Test
    public void testFieldTypesNoConflict() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(FieldTypes1.class, FieldTypes2.class, FieldTypes3.class);

        FieldTypes1 ft1;
        FieldTypes2 ft2;
        FieldTypes3 ft3;

        final JTransaction jtx1 = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx1);
        try {

            ft1 = jtx1.create(FieldTypes1.class);
            ft2 = jtx1.create(FieldTypes2.class);
            ft3 = jtx1.create(FieldTypes3.class);

            ft1.setField1(123);
            ft1.setField2(null);
            ft1.getField3().add("ft1.field3");

            ft2.getField1().add("ft2.field1");
            ft2.setField2(456);
            ft2.setField3(ft2);

            ft3.setField1(ft1);
            ft3.getField2().add("ft3.field3");
            ft3.setField3(789);

            jtx1.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        final JTransaction jtx2 = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx2);
        try {

            ft1 = jtx2.get(ft1);
            ft2 = jtx2.get(ft2);
            ft3 = jtx2.get(ft3);

            Assert.assertEquals(ft1.getField1(), 123);
            Assert.assertNull(ft1.getField2());
            Assert.assertEquals(ft1.getField3(), Arrays.asList("ft1.field3"));

            Assert.assertEquals(ft2.getField1(), Arrays.asList("ft2.field1"));
            Assert.assertEquals(ft2.getField2(), 456);
            Assert.assertSame(ft2.getField3(), ft2);

            Assert.assertSame(ft3.getField1(), ft1);
            Assert.assertEquals(ft3.getField2(), Arrays.asList("ft3.field3"));
            Assert.assertEquals(ft3.getField3(), 789);

            jtx2.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testFieldTypesConflict() {

        try {
            BasicTest.getJSimpleDB(Conflictor1.class, Conflictor2.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            BasicTest.getJSimpleDB(Conflictor3.class, Conflictor4.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    @Test
    public void testFieldTypesNoConflict2() {
        BasicTest.getJSimpleDB(NonConflictor1.class, NonConflictor2.class);
    }

// Model Classes

    @JSimpleClass
    public abstract static class FieldTypes1 implements JObject {

        public abstract int getField1();
        public abstract void setField1(int x);

        public abstract JObject getField2();
        public abstract void setField2(JObject x);

        public abstract List<String> getField3();

        public abstract Counter getField4();
    }

    @JSimpleClass
    public abstract static class FieldTypes2 implements JObject {

        public abstract List<String> getField1();

        public abstract int getField2();
        public abstract void setField2(int x);

        public abstract JObject getField3();
        public abstract void setField3(JObject x);

        public abstract Map<Short, Double> getField4();
    }

    @JSimpleClass
    public abstract static class FieldTypes3 implements JObject {

        public abstract JObject getField1();
        public abstract void setField1(JObject x);

        public abstract List<String> getField2();

        public abstract int getField3();
        public abstract void setField3(int x);

        public abstract Set<FieldTypes3> getField4();
    }

// Conflicting Model Classes

    @JSimpleClass
    public abstract static class Conflictor1 implements JObject {
        @JField(indexed = true)
        public abstract int getField1();
        public abstract void setField1(int x);
    }

    @JSimpleClass
    public abstract static class Conflictor2 implements JObject {
        @JField(indexed = true)
        public abstract String getField1();
        public abstract void setField1(String x);
    }

// Note map key fields must be congruent, even if only value field is indexed (because the key is part of the index)

    @JSimpleClass
    public abstract static class Conflictor3 implements JObject {
        @JMapField(value = @JField(indexed = true))
        public abstract Map<Float, String> getField1();
    }

    @JSimpleClass
    public abstract static class Conflictor4 implements JObject {
        @JMapField(value = @JField(indexed = true))
        public abstract Map<Double, String> getField1();
    }

// But map value fields can be different, if only key field is indexed

    @JSimpleClass
    public abstract static class NonConflictor1 implements JObject {
        @JMapField(key = @JField(indexed = true))
        public abstract Map<String, Float> getField1();
    }

    @JSimpleClass
    public abstract static class NonConflictor2 implements JObject {
        @JMapField(key = @JField(indexed = true))
        public abstract Map<String, Double> getField1();
    }
}
