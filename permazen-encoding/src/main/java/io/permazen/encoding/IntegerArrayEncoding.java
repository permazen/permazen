
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.primitives.Ints;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * {@code int[]} primitive array type. Does not support null arrays.
 */
public class IntegerArrayEncoding extends IntegralArrayEncoding<int[], Integer> {

    private static final long serialVersionUID = 2097437088172327725L;

    public IntegerArrayEncoding(EncodingId encodingId) {
       super(encodingId, new IntegerEncoding(null), int[].class);
    }

    @Override
    public IntegerArrayEncoding withEncodingId(EncodingId encodingId) {
        return new IntegerArrayEncoding(encodingId);
    }

    @Override
    protected int getArrayLength(int[] array) {
        return array.length;
    }

    @Override
    protected Integer getArrayElement(int[] array, int index) {
        return array[index];
    }

    @Override
    protected int[] createArray(List<Integer> elements) {
        return Ints.toArray(elements);
    }

    @Override
    protected void encode(int[] array, DataOutputStream output) throws IOException {
        for (int value : array)
            output.writeInt(value);
    }

    @Override
    protected int[] decode(DataInputStream input, int numBytes) throws IOException {
        final int[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readInt();
        return array;
    }
}
