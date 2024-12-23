
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnsignedIntEncoderTest extends TestSupport {

    @Test(dataProvider = "randomEncodings")
    public void testRandomUnsignedIntEncoding(String string) {
        final int value = this.decode(string);
        final ByteData encoding = this.encode(value);
        int expectedLength;
        if (value < UnsignedIntEncoder.MIN_MULTI_BYTE_VALUE)
            expectedLength = 1;
        else {
            final int adjusted = value - UnsignedIntEncoder.MIN_MULTI_BYTE_VALUE;
            if (/***/adjusted < 0x00000100)
                expectedLength = 2;
            else if (adjusted < 0x00010000)
                expectedLength = 3;
            else if (adjusted < 0x01000000)
                expectedLength = 4;
            else
                expectedLength = 5;
        }
        Assert.assertEquals(encoding.size(), expectedLength);
        Assert.assertEquals(encoding.size(), UnsignedIntEncoder.encodeLength(value));
        Assert.assertEquals(UnsignedIntEncoder.decodeLength(encoding.byteAt(0)), encoding.size());
        int decoding = this.decode(encoding);
        Assert.assertEquals(decoding, value);
    }

    public void testIllegalUnsignedIntEncodings() {

        try {
            this.decode(new byte[] { (byte)0xff });
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            this.decode(new byte[] { (byte)0xfe, (byte)0x80, (byte)0x00, (byte)0x00, (byte)0x00 });
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private int decode(String string) {
        int value = 0;
        for (byte b : ByteData.fromHex(string).toByteArray())
            value = (value << 8) | (b & 0xff);
        return value;
    }

    private int decode(byte[] buf) {
        return this.decode(ByteData.of(buf));
    }

    private int decode(ByteData data) {
        return UnsignedIntEncoder.read(data.newReader());
    }

    private ByteData encode(int value) {
        final ByteData.Writer writer = ByteData.newWriter(UnsignedIntEncoder.MAX_ENCODED_LENGTH);
        UnsignedIntEncoder.write(writer, value);
        return writer.toByteData();
    }

    @DataProvider(name = "randomEncodings")
    public String[][] genRandomEncodings() {
        final ArrayList<String[]> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final int maskBytes = this.random.nextInt(4) + 1;
            int value = this.random.nextInt() & 0x7fffffff;
            value &= ~0 >>> (maskBytes * 8);
            values.add(new String[] { String.format("%08x", value) });
        }
        return values.toArray(new String[values.size()][]);
    }
}
