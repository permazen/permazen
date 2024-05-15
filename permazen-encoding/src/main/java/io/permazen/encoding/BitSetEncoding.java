
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

import java.util.BitSet;

/**
 * Non-null {@link BitSet} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Instances are ordered as if they were giant unsigned integers, i.e., whichever instance has the highest bit set
 * to one where the other instance has it set to zero is considered bigger. So the empty instance is the smallest possible.
 */
public class BitSetEncoding extends AbstractEncoding<BitSet> {

    private static final long serialVersionUID = -1133774834687234873L;

    public BitSetEncoding() {
        super(BitSet.class);
    }

// Encoding

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public BitSet read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return BitSet.valueOf(this.reverse(reader.readBytes(UnsignedIntEncoder.read(reader))));
    }

    @Override
    public void write(ByteWriter writer, BitSet bitSet) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(bitSet != null);
        final byte[] bytes = bitSet.toByteArray();
        UnsignedIntEncoder.write(writer, bytes.length);
        writer.write(this.reverse(bytes));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(UnsignedIntEncoder.read(reader));
    }

    @Override
    public String toString(BitSet bitSet) {
        Preconditions.checkArgument(bitSet != null, "null bitSet");
        return "[" + ByteUtil.toString(this.reverse(bitSet.toByteArray())) + "]";
    }

    @Override
    public BitSet fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        Preconditions.checkArgument(string.matches("\\[([0-9a-f][0-9a-f])*\\]"), "invalid BitSet string");
        return BitSet.valueOf(this.reverse(ByteUtil.parse(string.substring(1, string.length() - 1))));
    }

    @Override
    public int compare(BitSet bitSet1, BitSet bitSet2) {
        int diff = Integer.compare(bitSet1.length(), bitSet2.length());
        if (diff != 0)
            return diff;
        final long[] longs1 = bitSet1.toLongArray();
        final long[] longs2 = bitSet2.toLongArray();
        assert longs1.length == longs2.length;
        for (int i = longs1.length - 1; i >= 0; i--) {
            if ((diff = Long.compareUnsigned(longs1[i], longs2[i])) != 0)
                return diff;
        }
        return 0;
    }

    private byte[] reverse(byte[] bytes) {
        int inc = 0;
        int dec = bytes.length - 1;
        while (inc < dec) {
            final byte lo = bytes[inc];
            final byte hi = bytes[dec];
            bytes[inc++] = hi;
            bytes[dec--] = lo;
        }
        return bytes;
    }
}
