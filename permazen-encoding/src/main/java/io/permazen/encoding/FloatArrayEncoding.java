
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Non-null {@code float[]} array type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Array elements are encoded using {@link FloatEncoding}, and the array is terminated by {@code 0x00000000},
 * which is an encoded value that can never be emitted by {@link FloatEncoding}.
 */
public class FloatArrayEncoding extends Base64ArrayEncoding<float[], Float> {

    private static final long serialVersionUID = 2791855034086017414L;

    private static final int NUM_BYTES = 4;
    private static final ByteData END = ByteData.zeros(NUM_BYTES);

    private final FloatEncoding floatType = new FloatEncoding(null);

    @SuppressWarnings("serial")
    public FloatArrayEncoding() {
        super(new FloatEncoding(null), new TypeToken<float[]>() { });
    }

    @Override
    public float[] read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final ArrayList<Float> list = new ArrayList<>();
        while (true) {
            final ByteData next = reader.readBytes(NUM_BYTES);
            if (next.equals(END))
                break;
            list.add(this.floatType.read(next.newReader()));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteData.Writer writer, float[] array) {
        Preconditions.checkArgument(array != null, "null array");
        Preconditions.checkArgument(writer != null);
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            this.floatType.write(writer, array[i]);
        writer.write(END);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        while (true) {
            final ByteData next = reader.readBytes(NUM_BYTES);
            if (next.equals(END))
                break;
        }
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
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
        for (float value : array)
            output.writeFloat(value);
    }

    @Override
    protected float[] decode(DataInputStream input, int numBytes) throws IOException {
        final float[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readFloat();
        return array;
    }
}
