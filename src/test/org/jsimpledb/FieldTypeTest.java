
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Date;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldTypeTest extends TestSupport {

    private final FieldTypeRegistry registry = new FieldTypeRegistry();

    @Test(dataProvider = "cases")
    public void testFieldType(String typeName, Object[] values) throws Exception {
        final FieldType<?> fieldType = registry.getFieldType(typeName);
        this.testFieldType2(fieldType, values);
    }

    @SuppressWarnings("unchecked")
    private <T> void testFieldType2(FieldType<T> fieldType, Object[] values) throws Exception {
        this.testFieldType3(fieldType, (T[])values);
    }

    private <T> void testFieldType3(FieldType<T> fieldType, T[] values) throws Exception {
        for (int i = 0; i < values.length; i++) {
            final T value = values[i];

            // Binary encoding
            final ByteWriter writer = new ByteWriter();
            fieldType.write(writer, value);
            final T value2 = fieldType.read(new ByteReader(writer));
            this.assertEquals(value2, value);
            this.assertEquals(fieldType.toString(value2), fieldType.toString(value));

            // String encoding
            final String s = fieldType.toString(value);
            final T value3 = fieldType.fromString(new ParseContext(s));
            this.assertEquals(value3, value);

            // Check sort order
            if (i > 0) {
                final T previous = values[i - 1];
                Assert.assertTrue(fieldType.compare(previous, value) < 0,
                  "sort failure: " + fieldType.toString(previous) + " < " + fieldType.toString(value));
            }
        }
    }

    private void assertEquals(Object actual, Object expected) {
        if (actual instanceof boolean[])
            Assert.assertEquals((boolean[])expected, (boolean[])actual);
        else if (actual instanceof byte[])
            Assert.assertEquals((byte[])expected, (byte[])actual);
        else if (actual instanceof char[])
            Assert.assertEquals((char[])expected, (char[])actual);
        else if (actual instanceof short[])
            Assert.assertEquals((short[])expected, (short[])actual);
        else if (actual instanceof int[])
            Assert.assertEquals((int[])expected, (int[])actual);
        else if (actual instanceof float[])
            Assert.assertEquals((float[])expected, (float[])actual);
        else if (actual instanceof long[])
            Assert.assertEquals((long[])expected, (long[])actual);
        else if (actual instanceof double[])
            Assert.assertEquals((double[])expected, (double[])actual);
        else if (actual instanceof Object[])
            Assert.assertEquals((Object[])expected, (Object[])actual);
        Assert.assertEquals(expected, actual);
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() throws Exception {
        return new Object[][] {

            {   "boolean", new Boolean[] {
                false, true
            }},

            {   "java.lang.Boolean", new Boolean[] {
                Boolean.FALSE, Boolean.TRUE, null
            }},

            {   "boolean[]", new boolean[][] {
                new boolean[] { },
                new boolean[] { false },
                new boolean[] { false, false },
                new boolean[] { false, false, false, },
                new boolean[] { false, false, true, },
                new boolean[] { true },
                new boolean[] { true, false, },
                new boolean[] { true, false, false, },
                new boolean[] { true, false, true, },
                new boolean[] { true, true, false, },
                new boolean[] { true, true, true, },
                new boolean[] { true, true, true, false },
                new boolean[] { true, true, true, false, false },
                new boolean[] { true, true, true, false, false, false },
                new boolean[] { true, true, true, false, false, true },
                new boolean[] { true, true, true, false, true },
                new boolean[] { true, true, true, false, true, false },
                new boolean[] { true, true, true, false, true, true },
                new boolean[] { true, true, true, true },
                new boolean[] { true, true, true, true, false },
                new boolean[] { true, true, true, true, false, false },
                new boolean[] { true, true, true, true, false, true },
                new boolean[] { true, true, true, true, true },
                new boolean[] { true, true, true, true, true, false },
                new boolean[] { true, true, true, true, true, true },
                null
            }},

            {   "boolean[][]", new boolean[][][] {
                { { } },
                { { }, { false } },
                { { }, { true } },
                { { false }, { } },
                { { false }, { false } },
                { { false }, { false, false } },
                { { true }, { } },
                { null },
                { null, { } },
                { null, { false } },
                { null, { false, true } },
                { null, { true } },
                { null, null },
                null
            }},

            {   "java.util.Date[]", new Date[][] {
                { },
                { new Date() },
                { null },
                { null, new Date(-1L) },
                { null, new Date(0L) },
                { null, new Date(1000000000L) },
                { null, new Date() },
                { null, null },
                null
            }},

            {   "java.lang.String", new String[] {
                "",
                "\u0000",
                "\u0000x",
                "\u0001",
                "\u0001x",
                "\u0002",
                "\u0002x",
                "foo",
                null
            }},

            {   "float[]", new float[][] {
                { },
                { Float.NEGATIVE_INFINITY },
                { -Float.MAX_VALUE },
                { -1.0f },
                { -Float.MIN_NORMAL },
                { -Float.MIN_VALUE },
                { -0.0f },
                { 0.0f },
                { Float.MIN_VALUE },
                { Float.MIN_NORMAL },
                { 1.0f },
                { Float.MAX_VALUE },
                { Float.POSITIVE_INFINITY },
                { Float.intBitsToFloat(0xffffffff) },   // NaN
                null
            }},

            {   "double[]", new double[][] {
                { },
                { Double.NEGATIVE_INFINITY },
                { -Double.MAX_VALUE },
                { -1.0f },
                { -Double.MIN_NORMAL },
                { -Double.MIN_VALUE },
                { -0.0f },
                { 0.0f },
                { Double.MIN_VALUE },
                { Double.MIN_NORMAL },
                { 1.0f },
                { Double.MAX_VALUE },
                { Double.POSITIVE_INFINITY },
                { Double.longBitsToDouble(0xffffffffffffffffL) },   // NaN
                null
            }},

        };
    }
}

