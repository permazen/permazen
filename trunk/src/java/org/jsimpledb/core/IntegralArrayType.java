
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.util.ArrayList;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;

/**
 * Array type for integral primitive element types. Does not support null arrays.
 *
 * <p>
 * Arrays are encoded as a sequence of numerical values followed by {@code 0x00}, where each value is encoded via
 * {@link LongEncoder}. Note, we take advantage of the fact that {@link LongEncoder} does not emit values starting
 * with {@code 0x00}.
 * </p>
 */
abstract class IntegralArrayType<T, E extends Number> extends ArrayType<T, E> {

    private static final int END = 0x00;

    private final IntegralType<E> integralType;

    @SuppressWarnings("serial")
    IntegralArrayType(IntegralType<E> elementType, Class<T> arrayClass) {
        super(elementType, TypeToken.of(arrayClass));
        if (this.elementType.hasPrefix0x00())
            throw new RuntimeException("internal error");
        this.integralType = elementType;
    }

    @Override
    public T read(ByteReader reader) {
        final ArrayList<E> list = new ArrayList<>();
        while (true) {
            final int first = reader.peek();
            if (first == END) {
                reader.skip(1);
                break;
            }
            list.add(this.integralType.downCast(LongEncoder.read(reader)));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteWriter writer, T array) {
        if (array == null)
            throw new IllegalArgumentException("null array");
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            LongEncoder.write(writer, this.integralType.upCast(this.getArrayElement(array, i)));
        writer.writeByte(END);
    }

    @Override
    public void skip(ByteReader reader) {
        while (true) {
            final int first = reader.peek();
            if (first == END)
                break;
            reader.skip(LongEncoder.decodeLength(first));
        }
    }

    @Override
    public byte[] getDefaultValue() {
        return new byte[] { (byte)END };
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.integralType.hasPrefix0xff();
    }
}

