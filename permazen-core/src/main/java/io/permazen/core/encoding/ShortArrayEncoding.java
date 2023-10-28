
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.primitives.Shorts;

import io.permazen.core.Encodings;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * {@code short[]} primitive array type. Does not support null arrays.
 */
public class ShortArrayEncoding extends IntegralArrayEncoding<short[], Short> {

    private static final long serialVersionUID = 2001467018347663363L;

    public ShortArrayEncoding() {
       super(Encodings.SHORT, short[].class);
    }

    @Override
    protected int getArrayLength(short[] array) {
        return array.length;
    }

    @Override
    protected Short getArrayElement(short[] array, int index) {
        return array[index];
    }

    @Override
    protected short[] createArray(List<Short> elements) {
        return Shorts.toArray(elements);
    }

    @Override
    protected void encode(short[] array, DataOutputStream output) throws IOException {
        for (short value : array)
            output.writeShort(value);
    }

    @Override
    protected short[] decode(DataInputStream input, int numBytes) throws IOException {
        final short[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readShort();
        return array;
    }
}
