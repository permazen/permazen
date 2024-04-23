
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import java.math.BigInteger;

/**
 * {@link BigInteger} type. Null values are not supported by this class.
 */
public class BigIntegerEncoding extends AbstractEncoding<BigInteger> {

    private static final long serialVersionUID = -2984648309356838144L;
    private static final int MAX_NUM_BYTES = (Integer.MAX_VALUE / Byte.SIZE) + 1;

    public BigIntegerEncoding(EncodingId encodingId) {
        super(encodingId, BigInteger.class, BigInteger.ZERO);
    }

// Encoding

    @Override
    public BigIntegerEncoding withEncodingId(EncodingId encodingId) {
        return new BigIntegerEncoding(encodingId);
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public BigInteger read(ByteReader reader) {
        final int numBytes = this.readSignedNumBytes(reader);
        return numBytes == 0 ? BigInteger.ZERO : new BigInteger(reader.readBytes(numBytes < 0 ? -numBytes : numBytes));
    }

    @Override
    public void write(ByteWriter writer, BigInteger value) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(value != null);
        if (value.signum() == 0) {
            LongEncoder.write(writer, 0);
            return;
        }
        final byte[] bytes = value.toByteArray();
        assert bytes.length <= MAX_NUM_BYTES;
        assert ((bytes[0] & 0x80) != 0) == (value.signum() < 0);
        LongEncoder.write(writer, value.signum() < 0 ? -bytes.length : bytes.length);
        writer.write(bytes);
    }

    @Override
    public void skip(ByteReader reader) {
        final int numBytes = this.readSignedNumBytes(reader);
        if (numBytes > 0)
            reader.skip(numBytes);
        else if (numBytes < 0)
            reader.skip(-numBytes);
    }

    // Read flag value (number of bytes * signum) and validate the sign bit on the first (high-order) byte is consistent with it
    private int readSignedNumBytes(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        final long flag = LongEncoder.read(reader);
        Preconditions.checkArgument(flag >= -MAX_NUM_BYTES && flag <= MAX_NUM_BYTES, "invalid BigInteger flag byte");
        if (flag != 0) {
            final int firstByte = reader.peek();
            Preconditions.checkArgument(((firstByte & 0x80) != 0) == (flag < 0), "invalid encoded BigInteger");
        }
        return (int)flag;
    }

    @Override
    public String toString(BigInteger value) {
        Preconditions.checkArgument(value != null);
        return value.toString();
    }

    @Override
    public BigInteger fromString(String string) {
        Preconditions.checkArgument(string != null);
        return new BigInteger(string);
    }

    @Override
    public int compare(BigInteger value1, BigInteger value2) {
        return value1.compareTo(value2);
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }
}
