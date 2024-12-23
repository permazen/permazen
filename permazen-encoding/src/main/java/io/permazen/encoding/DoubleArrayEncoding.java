
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Non-null {@code double[]} array type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Array elements are encoded using {@link DoubleEncoding}, and the array is terminated by {@code 0x0000000000000000L},
 * which is an encoded value that can never be emitted by {@link DoubleEncoding}.
 */
public class DoubleArrayEncoding extends Base64ArrayEncoding<double[], Double> {

    private static final long serialVersionUID = 7502947488125172882L;

    private static final int NUM_BYTES = 8;
    private static final ByteData END = ByteData.zeros(NUM_BYTES);

    private final DoubleEncoding doubleType = new DoubleEncoding(null);

    @SuppressWarnings("serial")
    public DoubleArrayEncoding() {
        super(new DoubleEncoding(null), new TypeToken<double[]>() { });
    }

    @Override
    public double[] read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final ArrayList<Double> list = new ArrayList<>();
        while (true) {
            final ByteData next = reader.readBytes(NUM_BYTES);
            if (next.equals(END))
                break;
            list.add(this.doubleType.read(next.newReader()));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteData.Writer writer, double[] array) {
        Preconditions.checkArgument(array != null, "null array");
        Preconditions.checkArgument(writer != null);
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            this.doubleType.write(writer, array[i]);
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
        return this.doubleType.hasPrefix0xff();
    }

    @Override
    protected int getArrayLength(double[] array) {
        return array.length;
    }

    @Override
    protected Double getArrayElement(double[] array, int index) {
        return array[index];
    }

    @Override
    protected double[] createArray(List<Double> elements) {
        return Doubles.toArray(elements);
    }

    @Override
    protected void encode(double[] array, DataOutputStream output) throws IOException {
        for (double value : array)
            output.writeDouble(value);
    }

    @Override
    protected double[] decode(DataInputStream input, int numBytes) throws IOException {
        final double[] array = this.checkDecodeLength(numBytes);
        for (int i = 0; i < array.length; i++)
            array[i] = input.readDouble();
        return array;
    }
}
