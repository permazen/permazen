
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LongEncoderTest extends TestSupport {

    @Test(dataProvider = "randomEncodings")
    public void testRandomLongEncoding(String string) {
        final long value = this.decode(string);
        final ByteData encoding = this.encode(value);
        Assert.assertEquals(encoding.size(), LongEncoder.encodeLength(value));
        Assert.assertEquals(LongEncoder.decodeLength(encoding.byteAt(0)), encoding.size());
        final long decoding = this.decode(encoding);
        Assert.assertEquals(decoding, value);
    }

    @Test(dataProvider = "encodings")
    public void testLongEncoding(long value, String string) {

        // Get expected encoding
        ByteData expected = ByteData.fromHex(string);

        // Test encoding
        final ByteData actual = this.encode(value);
        Assert.assertEquals(actual, expected);

        // Test encodeLength()
        Assert.assertEquals(actual.size(), LongEncoder.encodeLength(value));

        // Test decoding
        long value2 = this.decode(actual);
        Assert.assertEquals(value2, value);

        // Test decodeLength()
        Assert.assertEquals(actual.size(), LongEncoder.decodeLength(actual.byteAt(0)));
        Assert.assertEquals(actual.size(), LongEncoder.decodeLength(actual.ubyteAt(0)));
    }

    @Test(dataProvider = "lengths")
    public void testLongEncodeLength(long value, int expected) {
        final int actual = LongEncoder.encodeLength(value);
        Assert.assertEquals(actual, expected);
        final ByteData buf = this.encode(value);
        Assert.assertEquals(buf.size(), expected);
    }

    private long decode(String string) {
        long value = 0;
        for (byte b : ByteData.fromHex(string).toByteArray())
            value = (value << 8) | (b & 0xff);
        return value;
    }

    private long decode(ByteData buf) {
        return LongEncoder.read(buf.newReader());
    }

    private ByteData encode(long value) {
        final ByteData.Writer writer = ByteData.newWriter(LongEncoder.MAX_ENCODED_LENGTH);
        LongEncoder.write(writer, value);
        return writer.toByteData();
    }

    @DataProvider(name = "randomEncodings")
    public String[][] genRandomEncodings() {
        final ArrayList<String[]> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final int maskBytes = this.random.nextInt(8) + 1;
            long value = this.random.nextLong();
            if (value < 0)
                value |= ~0L << (maskBytes * 8);
            else
                value &= ~0L >>> (maskBytes * 8);
            values.add(new String[] { String.format("%016x", value) });
        }
        return values.toArray(new String[values.size()][]);
    }

    @DataProvider(name = "encodings")
    public Object[][] genEncodings() {
        return new Object[][] {

            // Corner cases
            { 0x8000000000000000L, "018000000000000076" },
            { 0xfeffffffffffff89L, "01feffffffffffffff" },
            { 0xfeffffffffffff8aL, "0200000000000000" },
            { 0xff00000000000000L, "0200000000000076" },
            { 0xfffeffffffffff89L, "02feffffffffffff" },
            { 0xfffeffffffffff8aL, "03000000000000" },
            { 0xffff000000000000L, "03000000000076" },
            { 0xfffffeffffffff89L, "03feffffffffff" },
            { 0xfffffeffffffff8aL, "040000000000" },
            { 0xffffff0000000000L, "040000000076" },
            { 0xfffffffeffffff89L, "04feffffffff" },
            { 0xfffffffeffffff8aL, "0500000000" },
            { 0xffffffff00000000L, "0500000076" },
            { 0xfffffffffeffff89L, "05feffffff" },
            { 0xfffffffffeffff8aL, "06000000" },
            { 0xffffffffff000000L, "06000076" },
            { 0xfffffffffffeff89L, "06feffff" },
            { 0xfffffffffffeff8aL, "070000" },
            { 0xffffffffffff0000L, "070076" },
            { 0xfffffffffffffe89L, "07feff" },
            { 0xfffffffffffffe8aL, "0800" },
            { 0xffffffffffffff00L, "0876" },
            { 0xffffffffffffff89L, "08ff" },
            { 0xffffffffffffff8aL, "09" },
            { 0xffffffffffffffa9L, "28" },
            { 0xffffffffffffffc9L, "48" },
            { 0xffffffffffffffe9L, "68" },
            { 0xffffffffffffffffL, "7e" },
            { 0x0000000000000000L, "7f" },
            { 0x0000000000000001L, "80" },
            { 0x0000000000000071L, "f0" },
            { 0x0000000000000077L, "f6" },
            { 0x0000000000000078L, "f700" },
            { 0x0000000000000177L, "f7ff" },
            { 0x0000000000000178L, "f80100" },
            { 0x0000000000010077L, "f8ffff" },
            { 0x0000000000010078L, "f9010000" },
            { 0x0000000001000077L, "f9ffffff" },
            { 0x0000000001000078L, "fa01000000" },
            { 0x0000000100000077L, "faffffffff" },
            { 0x0000000100000078L, "fb0100000000" },
            { 0x0000010000000077L, "fbffffffffff" },
            { 0x0000010000000078L, "fc010000000000" },
            { 0x0001000000000077L, "fcffffffffffff" },
            { 0x0001000000000078L, "fd01000000000000" },
            { 0x0100000000000077L, "fdffffffffffffff" },
            { 0x0100000000000078L, "fe0100000000000000" },
            { 0x7fffffffffffff78L, "fe7fffffffffffff00" },
            { 0x7fffffffffffffffL, "fe7fffffffffffff87" },

            // Other cases
            { 0xffffffff80000000L, "0580000076" },
            { 0xfffffffffffefe8aL, "06feff00" },
            { 0xfffffffffffefe8bL, "06feff01" },
            { 0xfffffffffffeff87L, "06fefffd" },
            { 0xfffffffffffeff88L, "06fefffe" },
            { 0xfffffffffffeff89L, "06feffff" },
            { 0xfffffffffffeff8aL, "070000" },
            { 0xfffffffffffeff8bL, "070001" },
            { 0xffffffffffff0087L, "0700fd" },
            { 0xffffffffffff0088L, "0700fe" },
            { 0xffffffffffff0089L, "0700ff" },
            { 0xffffffffffff008aL, "070100" },
            { 0xfffffffffffffe87L, "07fefd" },
            { 0xfffffffffffffe88L, "07fefe" },
            { 0xfffffffffffffe8aL, "0800" },
            { 0xfffffffffffffe8bL, "0801" },
            { 0xffffffffffffff88L, "08fe" },
            { 0xffffffffffffff89L, "08ff" },
            { 0xffffffffffffff8aL, "09" },
            { 0xffffffffffffffffL, "7e" },
            { 0x0000000000000000L, "7f" },
            { 0x0000000000000001L, "80" },
            { 0x0000000000000077L, "f6" },
            { 0x0000000000000078L, "f700" },
            { 0x0000000000000175L, "f7fd" },

            { 0x0000000000000176L, "f7fe" },
            { 0x0000000000000177L, "f7ff" },
            { 0x0000000000000276L, "f801fe" },
            { 0x000000000000ff76L, "f8fefe" },
            { 0x000000000000ff77L, "f8feff" },
            { 0x000000000000ff78L, "f8ff00" },
            { 0x000000000000ff79L, "f8ff01" },
            { 0x0000000000010075L, "f8fffd" },
            { 0x0000000000010076L, "f8fffe" },
            { 0x0000000000010077L, "f8ffff" },
            { 0x0000000000010078L, "f9010000" },
            { 0x0000000000010079L, "f9010001" },
            { 0x0000000000010175L, "f90100fd" },
            { 0x0000000000010176L, "f90100fe" },
            { 0x000000007fffffffL, "fa7fffff87" },
            { 0x7f00000000000000L - LongEncoder.POSITIVE_ADJUST, "fe7f00000000000000" },
            { 0x7fffffffffffffffL, "fe7fffffffffffff87" },
        };
    }

    @DataProvider(name = "lengths")
    public Object[][] genLengths() {
        return new Object[][] {

            // Check cutoff values
            {   LongEncoder.CUTOFF_VALUES[ 0] - 1,      9   },
            {   LongEncoder.CUTOFF_VALUES[ 0],          8   },
            {   LongEncoder.CUTOFF_VALUES[ 1] - 1,      8   },
            {   LongEncoder.CUTOFF_VALUES[ 1],          7   },
            {   LongEncoder.CUTOFF_VALUES[ 2] - 1,      7   },
            {   LongEncoder.CUTOFF_VALUES[ 2],          6   },
            {   LongEncoder.CUTOFF_VALUES[ 3] - 1,      6   },
            {   LongEncoder.CUTOFF_VALUES[ 3],          5   },
            {   LongEncoder.CUTOFF_VALUES[ 4] - 1,      5   },
            {   LongEncoder.CUTOFF_VALUES[ 4],          4   },
            {   LongEncoder.CUTOFF_VALUES[ 5] - 1,      4   },
            {   LongEncoder.CUTOFF_VALUES[ 5],          3   },
            {   LongEncoder.CUTOFF_VALUES[ 6] - 1,      3   },
            {   LongEncoder.CUTOFF_VALUES[ 6],          2   },
            {   LongEncoder.CUTOFF_VALUES[ 7] - 1,      2   },
            {   LongEncoder.CUTOFF_VALUES[ 7],          1   },
            {   LongEncoder.CUTOFF_VALUES[ 8] - 1,      1   },
            {   LongEncoder.CUTOFF_VALUES[ 8],          2   },
            {   LongEncoder.CUTOFF_VALUES[ 9] - 1,      2   },
            {   LongEncoder.CUTOFF_VALUES[ 9],          3   },
            {   LongEncoder.CUTOFF_VALUES[10] - 1,      3   },
            {   LongEncoder.CUTOFF_VALUES[10],          4   },
            {   LongEncoder.CUTOFF_VALUES[11] - 1,      4   },
            {   LongEncoder.CUTOFF_VALUES[11],          5   },
            {   LongEncoder.CUTOFF_VALUES[12] - 1,      5   },
            {   LongEncoder.CUTOFF_VALUES[12],          6   },
            {   LongEncoder.CUTOFF_VALUES[13] - 1,      6   },
            {   LongEncoder.CUTOFF_VALUES[13],          7   },
            {   LongEncoder.CUTOFF_VALUES[14] - 1,      7   },
            {   LongEncoder.CUTOFF_VALUES[14],          8   },
            {   LongEncoder.CUTOFF_VALUES[15] - 1,      8   },
            {   LongEncoder.CUTOFF_VALUES[15],          9   },

            // Check some other values
            { Long.MIN_VALUE,                               9   },
            { Long.MAX_VALUE,                               9   },
            { (long)Integer.MIN_VALUE,                      5   },
            { (long)Integer.MAX_VALUE,                      5   },
            { (long)Short.MIN_VALUE,                        3   },
            { (long)Short.MAX_VALUE,                        3   },
            { (long)Byte.MIN_VALUE,                         2   },
            { (long)Byte.MAX_VALUE,                         2   },
            { 0,                                            1   },
        };
    }
}
