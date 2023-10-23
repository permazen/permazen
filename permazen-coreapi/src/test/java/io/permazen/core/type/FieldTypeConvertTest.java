
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.net.InetAddresses;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.FieldType;
import io.permazen.core.FieldTypeRegistry;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldTypeConvertTest extends CoreAPITestSupport {

    private static final Object FAIL = "<FAIL>";

    private final FieldTypeRegistry registry = new FieldTypeRegistry();

    @Test(dataProvider = "convertCases")
    public void testConvertFieldType(String typeName, Object[] values, Object[] cases) throws Exception {
        final FieldType<?> stype = this.registry.getFieldType(typeName);
        assert stype != null : "didn't find field type \"" + typeName + "\"";
        for (int i = 0; i < cases.length; i += 2) {
            final String targetTypeName = (String)cases[i];
            final Object[] targetValues = (Object[])cases[i + 1];
            final FieldType<?> dtype = this.registry.getFieldType(targetTypeName);
            assert dtype != null : "didn't find target field type \"" + targetTypeName + "\"";
            for (int j = 0; j < values.length; j++)
                this.check(stype, dtype, values[j], targetValues[j]);
        }
    }

    private <S, T> void check(FieldType<S> stype, FieldType<T> dtype, Object input, Object expected0) throws Exception {
        final T actual;
        final T expected = expected0 != FAIL ? dtype.validate(expected0) : null;
        try {
            actual = dtype.convert(stype, stype.validate(input));
            if (expected0 == FAIL) {
                throw new AssertionError("convert glitch: " + stype.getName() + " -> " + dtype.getName()
                  + "; value " + stype.toParseableString(stype.validate(input))
                  + "; expected failure"
                  + " but got " + dtype.toParseableString(dtype.validate(actual)));
            }
        } catch (IllegalArgumentException e) {
            if (expected0 != FAIL) {
                throw new AssertionError("convert failure: " + stype.getName() + " -> " + dtype.getName()
                  + "; value " + stype.toParseableString(stype.validate(input))
                  + "; expected " + dtype.toParseableString(expected)
                  + " but got " + e, e);
            }
            return;
        }
        FieldTypeTest.assertEquals(dtype, actual, expected, "convert mismatch: " + stype.getName() + " -> " + dtype.getName()
          + "; value " + stype.toParseableString(stype.validate(input))
          + "; expected " + dtype.toParseableString(expected)
          + " but got " + dtype.toParseableString(dtype.validate(actual)));
    }

    @DataProvider(name = "convertCases")
    public Object[][] genConvertCases() throws Exception {
        return new Object[][] {

            // Primitive types to other primitive types and String
            {
                "boolean",      new Object[] { false, true },
                new Object[] {
                    "byte",     new Object[] { (byte)0, (byte)1 },
                    "short",    new Object[] { (short)0, (short)1 },
                    "char",     new Object[] { (char)0, (char)1 },
                    "int",      new Object[] { 0, 1 },
                    "float",    new Object[] { (float)0, (float)1 },
                    "long",     new Object[] { (long)0, (long)1 },
                    "double",   new Object[] { (double)0, (double)1 },
                    "java.lang.String",
                                new Object[] { "false", "true" },
                }
            },

            {
                "byte",         new Object[] { (byte)0x80, (byte)0xff, (byte)0x00, (byte)0x7f },
                new Object[] {
                    "boolean",  new Object[] { true, true, false, true },
                    "short",    new Object[] { (short)(byte)0x80, (short)(byte)0xff, (short)(byte)0x00, (short)(byte)0x7f },
                    "char",     new Object[] { (char)(byte)0x80, (char)(byte)0xff, (char)(byte)0x00, (char)(byte)0x7f },
                    "int",      new Object[] { (int)(byte)0x80, (int)(byte)0xff, (int)(byte)0x00, (int)(byte)0x7f },
                    "float",    new Object[] { (float)(byte)0x80, (float)(byte)0xff, (float)(byte)0x00, (float)(byte)0x7f },
                    "long",     new Object[] { (long)(byte)0x80, (long)(byte)0xff, (long)(byte)0x00, (long)(byte)0x7f },
                    "double",   new Object[] { (double)(byte)0x80, (double)(byte)0xff, (double)(byte)0x00, (double)(byte)0x7f },
                    "java.lang.String",
                                new Object[] { "" + (byte)0x80, "" + (byte)0xff, "" + (byte)0x00, "" + (byte)0x7f },
                }
            },

            {
                "char",         new Object[] { '\u0000', '\u0001', 'a', '\u7fff', '\uffff' },
                new Object[] {
                    "boolean",  new Object[] { false, true, true, true, true },
                    "byte",     new Object[] { (byte)'\u0000', (byte)'\u0001', (byte)'a', (byte)'\u7fff', (byte)'\uffff' },
                    "short",    new Object[] { (short)'\u0000', (short)'\u0001', (short)'a', (short)'\u7fff', (short)'\uffff' },
                    "int",      new Object[] { (int)'\u0000', (int)'\u0001', (int)'a', (int)'\u7fff', (int)'\uffff' },
                    "float",    new Object[] { (float)'\u0000', (float)'\u0001', (float)'a', (float)'\u7fff', (float)'\uffff' },
                    "long",     new Object[] { (long)'\u0000', (long)'\u0001', (long)'a', (long)'\u7fff', (long)'\uffff' },
                    "double",   new Object[] { (double)'\u0000', (double)'\u0001', (double)'a', (double)'\u7fff', (double)'\uffff'},
                    "java.lang.String",
                                new Object[] { "" + '\u0000', "" + '\u0001', "" + 'a', "" + '\u7fff', "" + '\uffff' },
                }
            },

            {
                "short",        new Object[] { (short)0xffff, (short)0x8000, (short)0x7fff, (short)0x0000, (short)0x0001 },
                new Object[] {
                    "boolean",  new Object[] { true, true, true, false, true },
                    "byte",     new Object[] {
            (byte)(short)0xffff, (byte)(short)0x8000, (byte)(short)0x7fff, (byte)(short)0x0000, (byte)(short)0x0001 },
                    "char",     new Object[] {
            (char)(short)0xffff, (char)(short)0x8000, (char)(short)0x7fff, (char)(short)0x0000, (char)(short)0x0001 },
                    "int",      new Object[] {
            (int)(short)0xffff, (int)(short)0x8000, (int)(short)0x7fff, (int)(short)0x0000, (int)(short)0x0001 },
                    "float",    new Object[] {
            (float)(short)0xffff, (float)(short)0x8000, (float)(short)0x7fff, (float)(short)0x0000, (float)(short)0x0001 },
                    "long",     new Object[] {
            (long)(short)0xffff, (long)(short)0x8000, (long)(short)0x7fff, (long)(short)0x0000, (long)(short)0x0001 },
                    "double",   new Object[] {
            (double)(short)0xffff, (double)(short)0x8000, (double)(short)0x7fff, (double)(short)0x0000, (double)(short)0x0001 },
                    "java.lang.String",
                                new Object[] {
            "" + (short)0xffff, "" + (short)0x8000, "" + (short)0x7fff, "" + (short)0x0000, "" + (short)0x0001 },
                }
            },

            {
                "int",          new Object[] { 0x800000, 0x7fffff, 0xffffff, 0, 1, 0xffff1234 },
                new Object[] {
                    "boolean",  new Object[] { true, true, true, false, true, true },
                    "byte",     new Object[]
            { (byte)0x800000, (byte)0x7fffff, (byte)0xffffff, (byte)0, (byte)1, (byte)0xffff1234 },
                    "short",    new Object[]
            { (short)0x800000, (short)0x7fffff, (short)0xffffff, (short)0, (short)1, (short)0xffff1234 },
                    "char",     new Object[]
            { (char)0x800000, (char)0x7fffff, (char)0xffffff, (char)0, (char)1, (char)0xffff1234 },
                    "float",    new Object[]
            { (float)0x800000, (float)0x7fffff, (float)0xffffff, (float)0, (float)1, (float)0xffff1234 },
                    "long",     new Object[]
            { (long)0x800000, (long)0x7fffff, (long)0xffffff, (long)0, (long)1, (long)0xffff1234 },
                    "double",   new Object[]
            { (double)0x800000, (double)0x7fffff, (double)0xffffff, (double)0, (double)1, (double)0xffff1234 },
                    "java.lang.String",
                                new Object[]
            { "" + 0x800000, "" + 0x7fffff, "" + 0xffffff, "" + 0, "" + 1, "" + 0xffff1234 },
                }
            },

            {
                "float",        new Object[]  { Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -1.0f,
                    -Float.MIN_NORMAL, -Float.MIN_VALUE, -0.0f, 0.0f, Float.MIN_VALUE, Float.MIN_NORMAL,
                    1.0f, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.intBitsToFloat(0xffffffff) },
                new Object[] {
                    "boolean",  new Object[] { true, true, true, true, true, false, false, true, true, true, true, true, true },
                    "byte",     new Object[]
                { (byte)Float.NEGATIVE_INFINITY, (byte)-Float.MAX_VALUE, (byte)-1.0f, (byte)-Float.MIN_NORMAL,
                    (byte)-Float.MIN_VALUE, (byte)-0.0f, (byte)0.0f, (byte)Float.MIN_VALUE, (byte)Float.MIN_NORMAL,
                    (byte)1.0f, (byte)Float.MAX_VALUE, (byte)Float.POSITIVE_INFINITY, (byte)Float.intBitsToFloat(0xffffffff) },
                    "short",    new Object[]
                { (short)Float.NEGATIVE_INFINITY, (short)-Float.MAX_VALUE, (short)-1.0f, (short)-Float.MIN_NORMAL,
                    (short)-Float.MIN_VALUE, (short)-0.0f, (short)0.0f, (short)Float.MIN_VALUE, (short)Float.MIN_NORMAL,
                    (short)1.0f, (short)Float.MAX_VALUE, (short)Float.POSITIVE_INFINITY, (short)Float.intBitsToFloat(0xffffffff) },
                    "char",     new Object[]
                { (char)Float.NEGATIVE_INFINITY, (char)-Float.MAX_VALUE, (char)-1.0f, (char)-Float.MIN_NORMAL,
                    (char)-Float.MIN_VALUE, (char)-0.0f, (char)0.0f, (char)Float.MIN_VALUE, (char)Float.MIN_NORMAL,
                    (char)1.0f, (char)Float.MAX_VALUE, (char)Float.POSITIVE_INFINITY, (char)Float.intBitsToFloat(0xffffffff) },
                    "int",    new Object[]
                { (int)Float.NEGATIVE_INFINITY, (int)-Float.MAX_VALUE, (int)-1.0f, (int)-Float.MIN_NORMAL,
                    (int)-Float.MIN_VALUE, (int)-0.0f, (int)0.0f, (int)Float.MIN_VALUE, (int)Float.MIN_NORMAL,
                    (int)1.0f, (int)Float.MAX_VALUE, (int)Float.POSITIVE_INFINITY, (int)Float.intBitsToFloat(0xffffffff) },
                    "long",     new Object[]
                { (long)Float.NEGATIVE_INFINITY, (long)-Float.MAX_VALUE, (long)-1.0f, (long)-Float.MIN_NORMAL,
                    (long)-Float.MIN_VALUE, (long)-0.0f, (long)0.0f, (long)Float.MIN_VALUE, (long)Float.MIN_NORMAL,
                    (long)1.0f, (long)Float.MAX_VALUE, (long)Float.POSITIVE_INFINITY, (long)Float.intBitsToFloat(0xffffffff) },
                    "double",   new Object[]
                { (double)Float.NEGATIVE_INFINITY, (double)-Float.MAX_VALUE, (double)-1.0f, (double)-Float.MIN_NORMAL,
                    (double)-Float.MIN_VALUE, (double)-0.0f, (double)0.0f, (double)Float.MIN_VALUE, (double)Float.MIN_NORMAL,
                    (double)1.0f, (double)Float.MAX_VALUE, (double)Float.POSITIVE_INFINITY,
                    (double)Float.intBitsToFloat(0xffffffff) },
                    "java.lang.String", new Object[]
                { "" + Float.NEGATIVE_INFINITY, "" + -Float.MAX_VALUE, "" + -1.0f, "" + -Float.MIN_NORMAL,
                    "" + -Float.MIN_VALUE, "" + -0.0f, "" + 0.0f, "" + Float.MIN_VALUE, "" + Float.MIN_NORMAL,
                    "" + 1.0f, "" + Float.MAX_VALUE, "" + Float.POSITIVE_INFINITY, "" + Float.intBitsToFloat(0xffffffff) },
                }
            },

            {
                "long",         new Object[] { Long.MIN_VALUE, -1L, 0L, 1L, 99L, Long.MAX_VALUE },
                new Object[] {
                    "boolean",  new Object[] { true, true, false, true, true, true },
                    "byte",     new Object[]
                { (byte)Long.MIN_VALUE, (byte)-1L, (byte)0L, (byte)1L, (byte)99L, (byte)Long.MAX_VALUE },
                    "short",    new Object[]
                { (short)Long.MIN_VALUE, (short)-1L, (short)0L, (short)1L, (short)99L, (short)Long.MAX_VALUE },
                    "char",     new Object[]
                { (char)Long.MIN_VALUE, (char)-1L, (char)0L, (char)1L, (char)99L, (char)Long.MAX_VALUE },
                    "int",      new Object[]
                { (int)Long.MIN_VALUE, (int)-1L, (int)0L, (int)1L, (int)99L, (int)Long.MAX_VALUE },
                    "float",    new Object[]
                { (float)Long.MIN_VALUE, (float)-1L, (float)0L, (float)1L, (float)99L, (float)Long.MAX_VALUE },
                    "double",   new Object[]
                { (double)Long.MIN_VALUE, (double)-1L, (double)0L, (double)1L, (double)99L, (double)Long.MAX_VALUE },
                    "java.lang.String",
                                new Object[]
                { "" + Long.MIN_VALUE, "" + -1L, "" + 0L, "" + 1L, "" + 99L, "" + Long.MAX_VALUE },
                },
            },

            {
                "double",       new Object[]  { Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1.0,
                    -Double.MIN_NORMAL, -Double.MIN_VALUE, -0.0, 0.0, Double.MIN_VALUE, Double.MIN_NORMAL,
                    1.0, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.longBitsToDouble(0xffffffffffffffffL) },
                new Object[] {
                    "boolean",  new Object[] { true, true, true, true, true, false, false, true, true, true, true, true, true },
                    "byte",     new Object[]
                { (byte)Double.NEGATIVE_INFINITY, (byte)-Double.MAX_VALUE, (byte)-1.0, (byte)-Double.MIN_NORMAL,
                    (byte)-Double.MIN_VALUE, (byte)-0.0, (byte)0.0, (byte)Double.MIN_VALUE, (byte)Double.MIN_NORMAL,
                    (byte)1.0, (byte)Double.MAX_VALUE, (byte)Double.POSITIVE_INFINITY,
                    (byte)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "short",    new Object[]
                { (short)Double.NEGATIVE_INFINITY, (short)-Double.MAX_VALUE, (short)-1.0, (short)-Double.MIN_NORMAL,
                    (short)-Double.MIN_VALUE, (short)-0.0, (short)0.0, (short)Double.MIN_VALUE, (short)Double.MIN_NORMAL,
                    (short)1.0, (short)Double.MAX_VALUE, (short)Double.POSITIVE_INFINITY,
                    (short)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "char",     new Object[]
                { (char)Double.NEGATIVE_INFINITY, (char)-Double.MAX_VALUE, (char)-1.0, (char)-Double.MIN_NORMAL,
                    (char)-Double.MIN_VALUE, (char)-0.0, (char)0.0, (char)Double.MIN_VALUE, (char)Double.MIN_NORMAL,
                    (char)1.0, (char)Double.MAX_VALUE, (char)Double.POSITIVE_INFINITY,
                    (char)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "int",    new Object[]
                { (int)Double.NEGATIVE_INFINITY, (int)-Double.MAX_VALUE, (int)-1.0, (int)-Double.MIN_NORMAL,
                    (int)-Double.MIN_VALUE, (int)-0.0, (int)0.0, (int)Double.MIN_VALUE, (int)Double.MIN_NORMAL,
                    (int)1.0, (int)Double.MAX_VALUE, (int)Double.POSITIVE_INFINITY,
                    (int)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "float",    new Object[]
                { (float)Double.NEGATIVE_INFINITY, (float)-Double.MAX_VALUE, (float)-1.0, (float)-Double.MIN_NORMAL,
                    (float)-Double.MIN_VALUE, (float)-0.0, (float)0.0, (float)Double.MIN_VALUE, (float)Double.MIN_NORMAL,
                    (float)1.0, (float)Double.MAX_VALUE, (float)Double.POSITIVE_INFINITY,
                    (float)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "long",     new Object[]
                { (long)Double.NEGATIVE_INFINITY, (long)-Double.MAX_VALUE, (long)-1.0, (long)-Double.MIN_NORMAL,
                    (long)-Double.MIN_VALUE, (long)-0.0, (long)0.0, (long)Double.MIN_VALUE, (long)Double.MIN_NORMAL,
                    (long)1.0, (long)Double.MAX_VALUE, (long)Double.POSITIVE_INFINITY,
                    (long)Double.longBitsToDouble(0xffffffffffffffffL) },
                    "java.lang.String", new Object[]
                { "" + Double.NEGATIVE_INFINITY, "" + -Double.MAX_VALUE, "" + -1.0, "" + -Double.MIN_NORMAL,
                    "" + -Double.MIN_VALUE, "" + -0.0, "" + 0.0, "" + Double.MIN_VALUE, "" + Double.MIN_NORMAL,
                    "" + 1.0, "" + Double.MAX_VALUE, "" + Double.POSITIVE_INFINITY,
                    "" + Double.longBitsToDouble(0xffffffffffffffffL) },
                }
            },

            // String to primitive types
            {
                "java.lang.String",     new Object[] { "", "0", "1", "true", "a#ck4", "500", "3.4" },
                new Object[] {
                    "boolean",  new Object[] { FAIL, FAIL, FAIL, true, FAIL, FAIL, FAIL },
                    "byte",     new Object[] { FAIL, (byte)0, (byte)1, FAIL, FAIL, FAIL, FAIL, },
                    "short",    new Object[] { FAIL, (short)0, (short)1, FAIL, FAIL, (short)500, FAIL, },
                    "char",     new Object[] { FAIL, '0', '1', FAIL, FAIL, FAIL, FAIL, },
                    "int",      new Object[] { FAIL, 0, 1, FAIL, FAIL, 500, FAIL, },
                    "float",    new Object[] { FAIL, 0.0f, 1.0f, FAIL, FAIL, 500.0f, 3.4f },
                    "long",     new Object[] { FAIL, 0L, 1L, FAIL, FAIL, 500L, FAIL, },
                    "double",   new Object[] { FAIL, 0.0, 1.0, FAIL, FAIL, 500.0, 3.4 },
                }
            },

            // char and String of length one
            {
                "char",                 new Object[] { 'a', 'b', '\u0000' },
                new Object[] {
                    "java.lang.String", new Object[] { "a", "b", "\u0000" },
                }
            },
            {
                "java.lang.String",     new Object[] { "a", "b", "\u0000" },
                new Object[] {
                    "char",             new Object[] { 'a', 'b', '\u0000' },
                }
            },

            // String and char[] array
            {
                "char[]",               new Object[] { new char[0], new char[] { 'a', 'b', 'c' }, new char[] { '\u0001' } },
                new Object[] {
                    "java.lang.String", new Object[] { "", "abc", "\u0001" },
                }
            },
            {
                "java.lang.String",     new Object[] { "", "abc", "\u0001" },
                new Object[] {
                    "char[]",           new Object[] { new char[0], new char[] { 'a', 'b', 'c' }, new char[] { '\u0001' } },
                }
            },

            // Arrays
            {
                "int[][]",              new Object[] { new int[][] { null, { }, { 123, -789 }, { -128 } } },
                new Object[] {
                    "byte[][]",         new Object[] { new byte[][] { null, { }, { (byte)123, (byte)-789 }, { (byte)-128 } } },
                }
            },
            {
                "char[][]",             new Object[] { new char[][] { null, { 'a', 'b', 'c' } } },
                new Object[] {
                    "java.lang.String[]", new Object[] { new String[] { null, "abc" } },
                }
            },

            // Inet4Address
            {
                "java.net.Inet4Address", new Object[] {
                                            InetAddresses.forString("0.0.0.0"),
                                            InetAddresses.forString("10.7.7.32"),
                                            InetAddresses.forString("192.168.0.1"),
                                            InetAddresses.forString("255.255.255.255"),
                                            InetAddresses.forString("224.3.4.5"),
                                        },
                new Object[] {
                    "java.net.InetAddress", new Object[] {
                                            InetAddresses.forString("0.0.0.0"),
                                            InetAddresses.forString("10.7.7.32"),
                                            InetAddresses.forString("192.168.0.1"),
                                            InetAddresses.forString("255.255.255.255"),
                                            InetAddresses.forString("224.3.4.5"),
                                        },
                }
            },

            // Inet6Address
            {
                "java.net.Inet6Address", new Object[] {
                                            InetAddresses.forString("::0"),
                                            InetAddresses.forString("::1"),
                                            InetAddresses.forString("::0a07:0730"),
                                            InetAddresses.forString("::192.168.0.1"),
                                            InetAddresses.forString("::e003:0405"),
                                            InetAddresses.forString("::ffff:ffff"),
                                            InetAddresses.forString("2001:db8::1"),
                                            InetAddresses.forString("fe80::10ed:4b7f:24fd:ce78"),
                                            InetAddresses.forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                                        },
                new Object[] {
                    "java.net.InetAddress", new Object[] {
                                            InetAddresses.forString("::0"),
                                            InetAddresses.forString("::1"),
                                            InetAddresses.forString("::0a07:0730"),
                                            InetAddresses.forString("::192.168.0.1"),
                                            InetAddresses.forString("::e003:0405"),
                                            InetAddresses.forString("::ffff:ffff"),
                                            InetAddresses.forString("2001:db8::1"),
                                            InetAddresses.forString("fe80::10ed:4b7f:24fd:ce78"),
                                            InetAddresses.forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                                        },
                }
            },
        };
    }
}

