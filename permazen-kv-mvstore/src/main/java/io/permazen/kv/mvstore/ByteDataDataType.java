
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

/**
 * MVStore {@link DataType} implementation encoding {@link ByteData} objects.
 */
public final class ByteDataDataType implements DataType<ByteData> {

    public static final ByteDataDataType INSTANCE = new ByteDataDataType();

    private ByteDataDataType() {
    }

// DataType

    // Mostly copied from BasicDataType
    @Override
    public int binarySearch(ByteData key, Object storage, int size, int initialGuess) {
        final ByteData[] array = (ByteData[])storage;
        int lo = 0;
        int hi = size - 1;
        int x = initialGuess - 1;
        if (x < 0 || x > hi)
            x = hi >>> 1;
        while (lo <= hi) {
            int diff = this.compare(key, array[x]);
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
    public int compare(ByteData a, ByteData b) {
        return a.compareTo(b);
    }

    @Override
    public ByteData[] createStorage(int size) {
        return new ByteData[size];
    }

    @Override
    public int getMemory(ByteData value) {
        final int byteArrayLength = 8 * ((12 + value.size() + 7) / 8);  // ref: https://www.baeldung.com/java-size-of-object
        final int objectLength = 8 * ((12 + 8 + 4 + 4 + 7) / 8);
        return objectLength + byteArrayLength;
    }

    @Override
    public boolean isMemoryEstimationAllowed() {
        return true;
    }

// Writing

    @Override
    public void write(WriteBuffer buf, ByteData value) {
        final ByteData encodedSize = UnsignedIntEncoder.encode(value.size());
        buf.put(encodedSize.toByteArray())
          .put(value.toByteArray());
    }

    @Override
    public void write(WriteBuffer buf, Object storage, int len) {
        final ByteData[] array = (ByteData[])storage;
        for (int i = 0; i < len; i++)
            this.write(buf, array[i]);
    }

// Reading

    @Override
    public ByteData read(ByteBuffer buf) {
        final byte[] bytes = new byte[UnsignedIntEncoder.read(buf)];
        buf.get(bytes);
        return ByteData.of(bytes);
    }

    @Override
    public void read(ByteBuffer buf, Object storage, int len) {
        final ByteData[] array = (ByteData[])storage;
        for (int i = 0; i < len; i++)
            array[i] = this.read(buf);
    }
}
