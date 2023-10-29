
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.primitives.Bytes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * {@code byte[]} primitive array type. Does not support null arrays.
 */
public class ByteArrayEncoding extends IntegralArrayEncoding<byte[], Byte> {

    private static final long serialVersionUID = -5978203098536001843L;

    public ByteArrayEncoding() {
       super(new ByteEncoding(), byte[].class);
    }

    @Override
    protected int getArrayLength(byte[] array) {
        return array.length;
    }

    @Override
    protected Byte getArrayElement(byte[] array, int index) {
        return array[index];
    }

    @Override
    protected byte[] createArray(List<Byte> elements) {
        return Bytes.toArray(elements);
    }

    @Override
    protected void encode(byte[] array, DataOutputStream output) throws IOException {
        output.write(array, 0, array.length);
    }

    @Override
    protected byte[] decode(DataInputStream input, int numBytes) throws IOException {
        final byte[] data = new byte[numBytes];
        input.readFully(data);
        return data;
    }
}
