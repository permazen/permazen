
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.net.InetAddresses;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import jakarta.mail.internet.InternetAddress;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EncodingTest extends TestSupport {

    private final EncodingRegistry registry = new DefaultEncodingRegistry();

    @Test(dataProvider = "cases")
    public void testEncoding(String typeName, Object[] values) throws Exception {
        final EncodingId encodingId = EncodingIds.builtin(typeName);
        final Encoding<?> encoding = registry.getEncoding(encodingId);
        assert encoding != null : "didn't find \"" + typeName + "\"";
        this.testEncoding2(encoding, values);
    }

    @SuppressWarnings("unchecked")
    private <T> void testEncoding2(Encoding<T> encoding, Object[] values) throws Exception {
        this.testEncoding3(encoding, (T[])values);
    }

    private <T> void testEncoding3(Encoding<T> encoding, T[] values) throws Exception {
        final ByteData[] encodings = new ByteData[values.length];
        for (int i = 0; i < values.length; i++) {
            final T value = values[i];

            // Binary encoding
            final ByteData.Writer writer = ByteData.newWriter();
            encoding.write(writer, value);
            encodings[i] = writer.toByteData();
            final T value2 = encoding.read(encodings[i].newReader());
            this.assertEquals(encoding, value2, value);

            // String encoding
            if (value != null) {
                Assert.assertEquals(encoding.toString(value2), encoding.toString(value));
                final String s = encoding.toString(value);
                this.checkValidString(value, s);
                final T value3 = encoding.fromString(s);
                this.assertEquals(encoding, value3, value);
            } else {
                try {
                    encoding.toString(null);
                    assert false;
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }

            // "list" style string encoding for some primitive arrays
            if (encoding instanceof Base64ArrayEncoding) {
                final Base64ArrayEncoding<T, ?> arrayType = (Base64ArrayEncoding<T, ?>)encoding;

                // String encoding
                if (value != null) {
                    Assert.assertEquals(arrayType.toString(value2, false), arrayType.toString(value, false));
                    final String s = arrayType.toString(value, false);
                    final T value3 = arrayType.fromString(s);
                    this.assertEquals(arrayType, value3, value);
                } else {
                    try {
                        arrayType.toString(null, false);
                        assert false;
                    } catch (IllegalArgumentException e) {
                        // expected
                    }
                }
            }

            // "list" style string encoding for some primitive arrays
            if (encoding instanceof Base64ArrayEncoding) {
                final Base64ArrayEncoding<T, ?> arrayType = (Base64ArrayEncoding<T, ?>)encoding;

                // String encoding
                Assert.assertEquals(arrayType.toString(value2, false), arrayType.toString(value, false));
                final String s3 = arrayType.toString(value, false);
                this.checkValidString(value, s3);
                final T value5 = arrayType.fromString(s3);
                this.assertEquals(arrayType, value5, value);
            }

            // Check sort order
            if (i > 0) {
                final T previous = values[i - 1];
                final boolean bytesEqual = encodings[i - 1].compareTo(encodings[i]) == 0;
                final boolean bytesLessThan = encodings[i - 1].compareTo(encodings[i]) < 0;
                final boolean fieldEqual = encoding.compare(previous, value) == 0;
                final boolean fieldLessThan = encoding.compare(previous, value) < 0;

                final String vstr = value != null ? "\"" + encoding.toString(value) + "\"" : "null";
                final String pstr = previous != null ? "\"" + encoding.toString(previous) + "\"" : "null";

                Assert.assertTrue(bytesLessThan || bytesEqual, "Binary sort failure @ " + i + ": expected "
                  + pstr + " [" + ByteUtil.toString(encodings[i - 1]) + "] <= "
                  + vstr + " [" + ByteUtil.toString(encodings[i]) + "]");
                Assert.assertTrue(fieldLessThan || fieldEqual, "Java sort failure @ " + i + ": expected "
                  + pstr + " <= " + vstr);

                Assert.assertEquals(bytesEqual, fieldEqual, "equality mismatch @ " + i + ": "
                  + pstr + " and " + vstr);
                Assert.assertEquals(bytesLessThan, fieldLessThan, "less-than mismatch @ " + i + ": "
                  + pstr + " and " + vstr);
            }
        }
    }

    private void checkValidString(Object value, String s) {
        for (int i = 0; i < s.length(); i++) {
            final int ch = s.charAt(i);
            switch (ch) {
            case '\t':
            case '\n':
            case '\r':
                break;
            default:
                if (ch >= '\u0020' && ch <= '\ud7ff')
                    break;
                if (ch >= '\ue000' && ch <= '\uffdf')
                    break;
                assert false : String.format(
                  "string encoding \"%s\" of value %s contains illegal character 0x%04x at index %d", s, value, ch, i);
                break;
            }
        }
    }

    private <T> void assertEquals(Encoding<T> encoding, T actual, T expected) {
        EncodingTest.assertEquals(encoding, actual, expected, "equals check failed: " + actual + " != " + expected);
    }

    public static <T> void assertEquals(Encoding<T> encoding, T actual, T expected, String message) {
        Assert.assertEquals(encoding.compare(actual, expected), 0, message);
        if (actual instanceof boolean[])
            Assert.assertEquals((boolean[])expected, (boolean[])actual, message);
        else if (actual instanceof byte[])
            Assert.assertEquals((byte[])expected, (byte[])actual, message);
        else if (actual instanceof char[])
            Assert.assertEquals((char[])expected, (char[])actual, message);
        else if (actual instanceof short[])
            Assert.assertEquals((short[])expected, (short[])actual, message);
        else if (actual instanceof int[])
            Assert.assertEquals((int[])expected, (int[])actual, message);
        else if (actual instanceof float[])
            TestSupport.assertEquals((float[])expected, (float[])actual, message);
        else if (actual instanceof long[])
            Assert.assertEquals((long[])expected, (long[])actual, message);
        else if (actual instanceof double[])
            TestSupport.assertEquals((double[])expected, (double[])actual, message);
        else if (actual instanceof Object[])
            Assert.assertTrue(Arrays.deepEquals((Object[])expected, (Object[])actual), message);
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() throws Exception {
        return new Object[][] {

            {   "boolean", new Boolean[] {
                false, true
            }},

            {   "Boolean", new Boolean[] {
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

            {   "Date[]", new Date[][] {
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

            {   "String", new String[] {
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
                { -1.0 },
                { -Double.MIN_NORMAL },
                { -Double.MIN_VALUE },
                { -0.0 },
                { 0.0 },
                { Double.MIN_VALUE },
                { Double.MIN_NORMAL },
                { 1.0 },
                { Double.MAX_VALUE },
                { Double.POSITIVE_INFINITY },
                { Double.longBitsToDouble(0xffffffffffffffffL) },   // NaN
                null
            }},

            {   UUID.class.getSimpleName(), new UUID[] {
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

            {   URI.class.getSimpleName(), new URI[] {
                new URI("/foobar"),
                new URI("http://www.google.com/"),
                new URI("http://www.google.com/?q=permazen"),
                null
            }},

            {   File.class.getSimpleName(), new File[] {
                new File(".profile"),
                new File("/lost+found"),
                new File("/tmp/foo"),
                null
            }},

            {   Pattern.class.getSimpleName(), new Pattern[] {
                Pattern.compile("(foo)?(bar)?"),
                Pattern.compile("^.*([\\s@]+)$"),
                Pattern.compile("ab*c"),
                null
            }},

            {   Duration.class.getSimpleName(), this.genSorted(
                () -> Duration.ofSeconds((long)this.random.nextInt() << 32 + this.random.nextInt(), this.randomNano()),
                Duration.ZERO)
            },

            {   Instant.class.getSimpleName(), this.genSorted(
                () -> Instant.ofEpochSecond(this.random.nextInt(), this.randomNano()),
                Instant.now())
            },

            {   LocalDateTime.class.getSimpleName(), this.genSorted(
                () -> LocalDateTime.of(this.randomYear(), this.randomMonth(), this.randomDay(),
                  this.randomHour(), this.randomMinute(), this.randomSecond(), this.randomNano()),
                LocalDateTime.now())
            },

            {   LocalDate.class.getSimpleName(), this.genSorted(
                () -> LocalDate.of(this.randomYear(), this.randomMonth(), this.randomDay()),
                LocalDate.now())
            },

            {   LocalTime.class.getSimpleName(), this.genSorted(
                () -> LocalTime.of(this.randomHour(), this.randomMinute(), this.randomSecond(), this.randomNano()),
                LocalTime.now())
            },

            {   MonthDay.class.getSimpleName(), this.genSorted(
                () -> MonthDay.of(this.randomMonth(), this.randomDay()),
                MonthDay.now())
            },

            {   OffsetDateTime.class.getSimpleName(), this.genSorted(
                () -> OffsetDateTime.of(this.randomYear(), this.randomMonth(), this.randomDay(),
                  this.randomHour(), this.randomMinute(), this.randomSecond(), this.randomNano(), ZoneOffset.UTC),
                OffsetDateTime.now())
            },

            {   OffsetTime.class.getSimpleName(), this.genSorted(
                () -> OffsetTime.of(this.randomHour(), this.randomMinute(),
                  this.randomSecond(), this.randomNano(), ZoneOffset.UTC),
                OffsetTime.now())
            },

            {   Period.class.getSimpleName(), new Period[] {
                Period.of(-10, 0, 0),
                Period.ZERO,
                Period.of(0, 3, 17),
                Period.of(20, 3, 17),
            }},

            {   YearMonth.class.getSimpleName(), this.genSorted(
                () -> YearMonth.of(this.randomYear(), this.randomMonth()),
                YearMonth.now())
            },

            {   Year.class.getSimpleName(), this.genSorted(
                () -> Year.of(this.randomYear()),
                Year.now())
            },

            {   ZoneOffset.class.getSimpleName(), this.genSorted(
                () -> ZoneOffset.ofTotalSeconds(this.randomOffsetSeconds()),
                ZoneOffset.UTC,
                ZoneOffset.MIN,
                ZoneOffset.MAX)
            },

            {   ZonedDateTime.class.getSimpleName(), this.genSorted(
                () -> ZonedDateTime.of(this.randomYear(), this.randomMonth(), this.randomDay(), this.randomHour(),
                  this.randomMinute(), this.randomSecond(), this.randomNano(), ZoneOffset.UTC),
                ZonedDateTime.now())
            },

            {   InternetAddress.class.getSimpleName(), new InternetAddress[] {
                new InternetAddress("\"Abercrombie & Kent\" <safari@ak.com>"),
                new InternetAddress("Fred Example <fred@example.com>"),
                new InternetAddress("linus@kernel.org"),
                new InternetAddress("xxx+foobar@3com.net"),
            }},

            {   Inet4Address.class.getSimpleName(), new Inet4Address[] {
                (Inet4Address)InetAddresses.forString("0.0.0.0"),
                (Inet4Address)InetAddresses.forString("10.7.7.32"),
                (Inet4Address)InetAddresses.forString("192.168.0.1"),
                (Inet4Address)InetAddresses.forString("224.3.4.5"),
                (Inet4Address)InetAddresses.forString("255.255.255.255"),
            }},

            {   Inet6Address.class.getSimpleName(), new Inet6Address[] {
                (Inet6Address)InetAddresses.forString("::0"),
                (Inet6Address)InetAddresses.forString("::1"),
                (Inet6Address)InetAddresses.forString("::0a07:0730"),
                (Inet6Address)InetAddresses.forString("::192.168.0.1"),
                (Inet6Address)InetAddresses.forString("::e003:0405"),
                (Inet6Address)InetAddresses.forString("::ffff:ffff"),
                (Inet6Address)InetAddresses.forString("2001:db8::1"),
                (Inet6Address)InetAddresses.forString("fe80::10ed:4b7f:24fd:ce78"),
                (Inet6Address)InetAddresses.forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
            }},

            {   InetAddress.class.getSimpleName(), new InetAddress[] {
                InetAddresses.forString("0.0.0.0"),
                InetAddresses.forString("10.7.7.32"),
                InetAddresses.forString("192.168.0.1"),
                InetAddresses.forString("224.3.4.5"),
                InetAddresses.forString("255.255.255.255"),
                InetAddresses.forString("::0"),
                InetAddresses.forString("::1"),
                InetAddresses.forString("::0a07:0730"),
                InetAddresses.forString("::192.168.0.1"),
                InetAddresses.forString("::e003:0405"),
                InetAddresses.forString("::ffff:ffff"),
                InetAddresses.forString("2001:db8::1"),
                InetAddresses.forString("fe80::10ed:4b7f:24fd:ce78"),
                InetAddresses.forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
            }},

            {   BitSet.class.getSimpleName(), new BitSet[] {
                new BitSet(),
                BitSet.valueOf(new byte[] { (byte)0x01                          }),
                BitSet.valueOf(new byte[] { (byte)0x05, (byte)0x00              }),
                BitSet.valueOf(new byte[] { (byte)0x06, (byte)0x00, (byte)0x00  }),
                BitSet.valueOf(new byte[] { (byte)0x10,                         }),
                BitSet.valueOf(new byte[] { (byte)0xfe,                         }),
                BitSet.valueOf(new byte[] { (byte)0xff,                         }),
                BitSet.valueOf(new byte[] { (byte)0x00, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0x01, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0x7f, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0x80, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0xaa, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0xff, (byte)0x01,             }),
                BitSet.valueOf(new byte[] { (byte)0x12, (byte)0x34, (byte)0x56  }),
                BitSet.valueOf(new byte[] { (byte)0xff, (byte)0xff, (byte)0xfe  }),
                BitSet.valueOf(new byte[] { (byte)0xff, (byte)0xfe, (byte)0xff  }),
                BitSet.valueOf(new byte[] { (byte)0x00, (byte)0xff, (byte)0xff  }),
                BitSet.valueOf(new byte[] { (byte)0xfe, (byte)0xff, (byte)0xff  }),
                BitSet.valueOf(new byte[] { (byte)0xff, (byte)0xff, (byte)0xff  }),
            }},

            {   BigInteger.class.getSimpleName(), new BigInteger[] {
                new BigInteger("-9999999999999999999999999999999999999"),
                new BigInteger("-4089446911"),
                new BigInteger("-62915071"),
                new BigInteger("-62914817"),
                new BigInteger("-16711679"),
                new BigInteger("-65279"),
                new BigInteger("-257"),
                new BigInteger("-256"),
                new BigInteger("-254"),
                new BigInteger("-128"),
                new BigInteger("-23"),
                BigInteger.TEN.negate(),
                BigInteger.ONE.negate(),
                new BigInteger("-1"),
                BigInteger.ONE.negate(),
                new BigInteger("0"),
                BigInteger.ZERO,
                BigInteger.ONE,
                new BigInteger("1"),
                BigInteger.ONE,
                new BigInteger("2"),
                new BigInteger("3"),
                new BigInteger("7"),
                new BigInteger("8"),
                BigInteger.TEN,
                new BigInteger("16"),
                new BigInteger("63"),
                new BigInteger("255"),
                new BigInteger("256"),
                new BigInteger("496"),
                new BigInteger("511"),
                new BigInteger("65535"),
                new BigInteger("131844"),
                new BigInteger("7471876"),
                new BigInteger("7471877"),
                new BigInteger("16777215"),
                new BigInteger("16777216"),
                new BigInteger("2147483647"),
                new BigInteger("2147483648"),
                new BigInteger("2147483649"),
                new BigInteger("2863311530"),
                new BigInteger("4294967294"),
                new BigInteger("4294967295"),
                new BigInteger("9999999999999999999999999999999999999"),
            }},

            {   BigDecimal.class.getSimpleName(), new BigDecimal[] {
                new BigDecimal("-9999999999999999999999999999999999999e1000"),
                new BigDecimal("-9999999999999999999999999999999999999e100"),
                new BigDecimal("-9999999999999999999999999999999999999e10"),
                new BigDecimal("-9999999999999999999999999999999999999e1"),
                new BigDecimal("-9999999999999999999999999999999999999"),
                new BigDecimal("-999999999999999999999999999999999999"),
                new BigDecimal("-99999999999999999999999999999999"),
                new BigDecimal("-9999999999999999999999999999999.9"),
                new BigDecimal("-502.449999999999"),
                new BigDecimal("-502.4050010"),
                new BigDecimal("-502.405001"),
                new BigDecimal("-502.4050000000020000"),
                new BigDecimal("-502.4050000000000001"),
                new BigDecimal("-502.4050000000000000"),
                new BigDecimal("-502.405000"),
                new BigDecimal("-502.4050"),
                new BigDecimal("-502.405"),
                new BigDecimal("-501.99999999999999999"),
                new BigDecimal("-501.99999999999999999e-1"),
                new BigDecimal("-49.00000e0"),
                new BigDecimal("-49.000e0"),
                new BigDecimal("-49e0"),
                new BigDecimal("-1.00e1"),
                new BigDecimal("-10"),
                new BigDecimal("-50E-1"),
                new BigDecimal("-.4e1"),
                new BigDecimal("-3"),
                new BigDecimal("-2"),
                BigDecimal.ONE.negate(),
                new BigDecimal("-1"),
                BigDecimal.ONE.negate(),
                new BigDecimal("-.404"),
                new BigDecimal("-.400"),
                new BigDecimal("-.4"),
                new BigDecimal("-.3"),
                new BigDecimal("-1.3e-1"),
                new BigDecimal("-0.01"),
                new BigDecimal("-0.0001"),
                new BigDecimal("-1.5e-10"),
                new BigDecimal("-5.1e-11"),
                new BigDecimal("-5.1e-11234"),
                new BigDecimal("-5.1e-11234383"),
                BigDecimal.ZERO,
                new BigDecimal("0"),
                BigDecimal.ZERO,
                new BigDecimal("0.0"),
                new BigDecimal("0.00"),
                new BigDecimal("0.0000"),
                new BigDecimal("5.1e-11234383"),
                new BigDecimal("5.1e-11234"),
                new BigDecimal("5.1e-11"),
                new BigDecimal("1.5e-10"),
                new BigDecimal("0.0001"),
                new BigDecimal("0.01"),
                new BigDecimal("1.3e-1"),
                new BigDecimal(".3"),
                new BigDecimal(".4"),
                new BigDecimal(".400"),
                new BigDecimal(".404"),
                BigDecimal.ONE,
                new BigDecimal("1"),
                BigDecimal.ONE,
                new BigDecimal("2"),
                new BigDecimal("3"),
                new BigDecimal(".4e1"),
                new BigDecimal("50E-1"),
                new BigDecimal("10"),
                new BigDecimal("1.00e1"),
                new BigDecimal("49e0"),
                new BigDecimal("49.000e0"),
                new BigDecimal("49.00000e0"),
                new BigDecimal("501.99999999999999999e-1"),
                new BigDecimal("501.99999999999999999"),
                new BigDecimal("502.405"),
                new BigDecimal("502.4050"),
                new BigDecimal("502.405000"),
                new BigDecimal("502.4050000000000000"),
                new BigDecimal("502.4050000000000001"),
                new BigDecimal("502.4050000000020000"),
                new BigDecimal("502.405001"),
                new BigDecimal("502.4050010"),
                new BigDecimal("502.449999999999"),
                new BigDecimal("9999999999999999999999999999999.9"),
                new BigDecimal("99999999999999999999999999999999"),
                new BigDecimal("999999999999999999999999999999999999"),
                new BigDecimal("9999999999999999999999999999999999999"),
                new BigDecimal("9999999999999999999999999999999999999e1"),
                new BigDecimal("9999999999999999999999999999999999999e10"),
                new BigDecimal("9999999999999999999999999999999999999e100"),
                new BigDecimal("9999999999999999999999999999999999999e1000"),
            }},

        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <E extends Comparable> Object[] genSorted(Supplier<E> supplier, E... extras) {
        final Object[] array = new Object[extras.length + this.random.nextInt(100) + 50];
        System.arraycopy(extras, 0, array, 0, extras.length);
        for (int i = extras.length; i < array.length; i++)
            array[i] = supplier.get();
        Arrays.sort(array);
        return array;
    }

    private int randomYear() {
        return this.random.nextInt(4000 * 2) - 4000;
    }

    private int randomMonth() {
        return this.random.nextInt(12) + 1;
    }

    private int randomDay() {
        return this.random.nextInt(28) + 1;
    }

    private int randomHour() {
        return this.random.nextInt(24);
    }

    private int randomMinute() {
        return this.random.nextInt(60);
    }

    private int randomSecond() {
        return this.random.nextInt(60);
    }

    private int randomNano() {
        return this.random.nextInt(1000000000);
    }

    private int randomOffsetSeconds() {
        return this.random.nextInt(18 * 60 * 2 + 1) - 18 * 60;
    }
}
