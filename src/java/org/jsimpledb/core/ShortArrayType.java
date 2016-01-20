
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.primitives.Shorts;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * {@code short[]} primitive array type. Does not support null arrays.
 */
class ShortArrayType extends IntegralArrayType<short[], Short> {

    ShortArrayType() {
       super(FieldTypeRegistry.SHORT, 2, short[].class);
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
        for (int i = 0; i < array.length; i++)
            output.writeShort(array[i]);
    }

    @Override
    protected short[] decode(DataInputStream input, int numBytes) throws IOException {
        final short[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readShort();
        return array;
    }
}

