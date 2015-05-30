
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

import org.jsimpledb.TestSupport;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldTypeTest extends TestSupport {

    private final FieldTypeRegistry registry = new FieldTypeRegistry();

    @Test(dataProvider = "cases")
    public void testFieldType(String typeName, Object[] values) throws Exception {
        final FieldType<?> fieldType = registry.getFieldType(typeName);
        assert fieldType != null : "didn't find `" + typeName + "'";
        this.testFieldType2(fieldType, values);
    }

    @SuppressWarnings("unchecked")
    private <T> void testFieldType2(FieldType<T> fieldType, Object[] values) throws Exception {
        this.testFieldType3(fieldType, (T[])values);
    }

    private <T> void testFieldType3(FieldType<T> fieldType, T[] values) throws Exception {
        final byte[][] encodings = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            final T value = values[i];

            // Binary encoding
            final ByteWriter writer = new ByteWriter();
            fieldType.write(writer, value);
            encodings[i] = writer.getBytes();
            final T value2 = fieldType.read(new ByteReader(encodings[i]));
            this.assertEquals(fieldType, value2, value);

            // String encoding
            if (value != null) {
                Assert.assertEquals(fieldType.toString(value2), fieldType.toString(value));
                final String s = fieldType.toString(value);
                final T value3 = fieldType.fromString(s);
                this.assertEquals(fieldType, value3, value);
            } else {
                try {
                    fieldType.toString(null);
                    assert false;
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }

            // Parseable string encoding
            Assert.assertEquals(fieldType.toParseableString(value2), fieldType.toParseableString(value));
            final String s2 = fieldType.toParseableString(value);
            final ParseContext ctx = new ParseContext(s2 + ",abcd");
            final T value4 = fieldType.fromParseableString(ctx);
            this.assertEquals(fieldType, value4, value);
            Assert.assertEquals(ctx.getInput(), ",abcd");

            // Check sort order
            if (i > 0) {
                final T previous = values[i - 1];
                Assert.assertTrue(ByteUtil.compare(encodings[i - 1], encodings[i]) < 0,
                  "binary sort failure: " + fieldType.toParseableString(previous) + " < " + fieldType.toParseableString(value));
                Assert.assertTrue(fieldType.compare(previous, value) < 0,
                  "Java sort failure: " + fieldType.toParseableString(previous) + " < " + fieldType.toParseableString(value));
            }
        }
    }

    private <T> void assertEquals(FieldType<T> fieldType, T actual, T expected) {
        Assert.assertEquals(fieldType.compare(actual, expected), 0);
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

            {   "byte[]", new byte[][] {
                { },
                { (byte)0x80 },
                { (byte)0xe0 },
                { (byte)0xff },
                { (byte)0x00 },
                { (byte)0x01 },
                { (byte)0x7f },
                { (byte)0x7f, 0x00 },
                { (byte)0x7f, 0x00, 0x00 },
                null
            }},

            {   "char[]", new char[][] {
                { },
                { '\u0000' },
                { 'a' },
                { 'a', '\u0000' },
                { 'z', 'z' },
                null
            }},

            {   "short[]", new short[][] {
                { },
                { (short)0x8000 },
                { (short)0xffff },
                { (short)0x0000 },
                { (short)0x0001 },
                { (short)0x7fff },
                null
            }},

            {   "int[]", new int[][] {
                { },
                { Integer.MIN_VALUE },
                { -1 },
                { 0 },
                { 1 },
                { 1, 99 },
                { Integer.MAX_VALUE },
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

            {   "long[]", new long[][] {
                { },
                { Long.MIN_VALUE },
                { -1L },
                { 0L },
                { 1L },
                { 1L, 99L },
                { Long.MAX_VALUE },
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

            {   UUID.class.getName(), new UUID[] {
                UUID.fromString("89b3ed7f-5a7e-4604-9d42-2072248c91e7"),
                UUID.fromString("b9c069d9-8b09-445e-ad99-9f4b89a14779"),
                UUID.fromString("e82fd154-7027-479c-ba47-3d01374d82ad"),
                UUID.fromString("0e38c34f-eb22-4f6b-a6e7-8d910242a31a"),
                UUID.fromString("1ed9b4e3-766d-4b5f-864f-43fac6f869f6"),
                UUID.fromString("3bc5c507-06b0-40eb-9d1e-f1e704dd1461"),
                UUID.fromString("43940997-db56-4c66-9f11-1b6981eb2efe"),
                UUID.fromString("79b3ed7f-5a7e-9d42-8000-2072248c91e7"),
                UUID.fromString("79b3ed7f-5a7e-9d42-ffff-2072248c91e7"),
                UUID.fromString("79b3ed7f-5a7e-9d42-0000-2072248c91e7"),
                UUID.fromString("79b3ed7f-5a7e-9d42-7fff-2072248c91e7"),
                null
            }},

            {   URI.class.getName(), new URI[] {
                new URI("/foobar"),
                new URI("http://www.google.com/"),
                new URI("http://www.google.com/?q=jsimpledb"),
                null
            }},

            {   File.class.getName(), new File[] {
                new File(".profile"),
                new File("/lost+found"),
                new File("/tmp/foo"),
                null
            }},

            {   Pattern.class.getName(), new Pattern[] {
                Pattern.compile("(foo)?(bar)?"),
                Pattern.compile("^.*([\\s@]+)$"),
                Pattern.compile("ab*c"),
                null
            }},

        };
    }
}

