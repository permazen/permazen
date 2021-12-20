
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.util.ByteUtil;
import io.permazen.util.UnsignedIntEncoder;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

/**
 * MVStore {@link DataType} implementation encoding {@code byte[]} arrays sorted lexicographically.
 */
public final class ByteArrayDataType implements DataType<byte[]> {

    public static final ByteArrayDataType INSTANCE = new ByteArrayDataType();

    private ByteArrayDataType() {
    }

// DataType

    // Mostly copied from BasicDataType
    @Override
    public int binarySearch(byte[] key, Object storage, int size, int initialGuess) {
        final byte[][] array = (byte[][])storage;
        int lo = 0;
        int hi = size - 1;
        int x = initialGuess - 1;
        if (x < 0 || x > hi)
            x = hi >>> 1;
        while (lo <= hi) {
            int diff = compare(key, array[x]);
            if (diff > 0)
                lo = x + 1;
            else if (diff < 0)
                hi = x - 1;
            else
                return x;
            x = (lo + hi) >>> 1;
        }
        return ~lo;
    }

    @Override
    public int compare(byte[] a, byte[] b) {
        return ByteUtil.compare(a, b);
    }

    @Override
    public byte[][] createStorage(int size) {
        return new byte[size][];
    }

    @Override
    public int getMemory(byte[] value) {
        return 8 * ((12 + value.length + 7) / 8);           // ref: https://www.baeldung.com/java-size-of-object
    }

    @Override
    public boolean isMemoryEstimationAllowed() {
        return true;
    }

// Writing

    @Override
    public void write(WriteBuffer buf, byte[] value) {
        buf.put(UnsignedIntEncoder.encode(value.length))
          .put(value);
    }

    @Override
    public void write(WriteBuffer buf, Object storage, int len) {
        final byte[][] array = (byte[][])storage;
        for (int i = 0; i < len; i++)
            this.write(buf, array[i]);
    }

// Reading

    @Override
    public byte[] read(ByteBuffer buf) {
        final byte[] bytes = new byte[UnsignedIntEncoder.read(buf)];
        buf.get(bytes);
        return bytes;
    }

    @Override
    public void read(ByteBuffer buf, Object storage, int len) {
        final byte[][] array = (byte[][])storage;
        for (int i = 0; i < len; i++)
            array[i] = this.read(buf);
    }
}
