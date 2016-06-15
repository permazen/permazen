
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.google.common.reflect.TypeToken;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@code float[]} array type. Does not support null arrays.
 *
 * <p>
 * Array elements are encoded using {@link FloatType}, and the array is terminated by {@code 0x00000000},
 * which is an encoded value that can never be emitted by {@link FloatType}.
 */
class FloatArrayType extends Base64ArrayType<float[], Float> {

    private static final int NUM_BYTES = 4;
    private static final byte[] END = new byte[NUM_BYTES];

    private final FloatType floatType = new FloatType();

    @SuppressWarnings("serial")
    FloatArrayType() {
        super(FieldTypeRegistry.FLOAT, new TypeToken<float[]>() { });
    }

    @Override
    public float[] read(ByteReader reader) {
        final ArrayList<Float> list = new ArrayList<>();
        while (true) {
            final byte[] next = reader.readBytes(NUM_BYTES);
            if (Arrays.equals(next, END))
                break;
            list.add(this.floatType.read(new ByteReader(next)));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteWriter writer, float[] array) {
        Preconditions.checkArgument(array != null, "null array");
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            this.floatType.write(writer, array[i]);
        writer.write(END);
    }

    @Override
    public void skip(ByteReader reader) {
        while (true) {
            final byte[] next = reader.readBytes(NUM_BYTES);
            if (Arrays.equals(next, END))
                break;
        }
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.floatType.hasPrefix0xff();
    }

    @Override
    protected int getArrayLength(float[] array) {
        return array.length;
    }

    @Override
    protected Float getArrayElement(float[] array, int index) {
        return array[index];
    }

    @Override
    protected float[] createArray(List<Float> elements) {
        return Floats.toArray(elements);
    }

    @Override
    protected void encode(float[] array, DataOutputStream output) throws IOException {
        for (int i = 0; i < array.length; i++)
            output.writeFloat(array[i]);
    }

    @Override
    protected float[] decode(DataInputStream input, int numBytes) throws IOException {
        final float[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readFloat();
        return array;
    }
}

