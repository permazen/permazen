
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This test just proves that fields with the same name can have different types in different
 * classes as long as they are not both indexed.
 */
public class EncodingsConflictTest extends TestSupport {

    @Test
    public void testEncodingsNoConflict() {

        final Permazen jdb = BasicTest.getPermazen(Encodings1.class, Encodings2.class, Encodings3.class);

        Encodings1 ft1;
        Encodings2 ft2;
        Encodings3 ft3;

        final JTransaction jtx1 = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx1);
        try {

            ft1 = jtx1.create(Encodings1.class);
            ft2 = jtx1.create(Encodings2.class);
            ft3 = jtx1.create(Encodings3.class);

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
    public void testEncodingsConflict() {

        try {
            BasicTest.getPermazen(Conflictor1.class, Conflictor2.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            BasicTest.getPermazen(Conflictor3.class, Conflictor4.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    @Test
    public void testEncodingsNoConflict2() {
        BasicTest.getPermazen(NonConflictor1.class, NonConflictor2.class);
    }

// Model Classes

    @PermazenType
    public abstract static class Encodings1 implements JObject {

        public abstract int getField1();
        public abstract void setField1(int x);

        public abstract JObject getField2();
        public abstract void setField2(JObject x);

        public abstract List<String> getField3();

        public abstract Counter getField4();
    }

    @PermazenType
    public abstract static class Encodings2 implements JObject {

        public abstract List<String> getField1();

        public abstract int getField2();
        public abstract void setField2(int x);

        public abstract JObject getField3();
        public abstract void setField3(JObject x);

        public abstract Map<Short, Double> getField4();
    }

    @PermazenType
    public abstract static class Encodings3 implements JObject {

        public abstract JObject getField1();
        public abstract void setField1(JObject x);

        public abstract List<String> getField2();

        public abstract int getField3();
        public abstract void setField3(int x);

        public abstract Set<Encodings3> getField4();
    }

// Conflicting Model Classes

    @PermazenType
    public abstract static class Conflictor1 implements JObject {
        @JField(indexed = true)
        public abstract int getField1();
        public abstract void setField1(int x);
    }

    @PermazenType
    public abstract static class Conflictor2 implements JObject {
        @JField(indexed = true)
        public abstract String getField1();
        public abstract void setField1(String x);
    }

// Note map key fields must be congruent, even if only value field is indexed (because the key is part of the index)

    @PermazenType
    public abstract static class Conflictor3 implements JObject {
        @JMapField(value = @JField(indexed = true))
        public abstract Map<Float, String> getField1();
    }

    @PermazenType
    public abstract static class Conflictor4 implements JObject {
        @JMapField(value = @JField(indexed = true))
        public abstract Map<Double, String> getField1();
    }

// But map value fields can be different, if only key field is indexed

    @PermazenType
    public abstract static class NonConflictor1 implements JObject {
        @JMapField(key = @JField(indexed = true))
        public abstract Map<String, Float> getField1();
    }

    @PermazenType
    public abstract static class NonConflictor2 implements JObject {
        @JMapField(key = @JField(indexed = true))
        public abstract Map<String, Double> getField1();
    }
}
