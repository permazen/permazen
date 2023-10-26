
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import java.util.ArrayList;

/**
 * Array type for integral primitive element types. Does not support null arrays.
 *
 * <p>
 * Arrays are encoded as a sequence of numerical values followed by {@code 0x00}, where each value is encoded via
 * {@link LongEncoder}. Note, we take advantage of the fact that {@link LongEncoder} does not emit values starting
 * with {@code 0x00}.
 */
public abstract class IntegralArrayType<T, E extends Number> extends Base64ArrayType<T, E> {

    private static final long serialVersionUID = -5185297639150351646L;

    private static final int END = 0x00;

    private final IntegralType<E> integralType;

    @SuppressWarnings("serial")
    protected IntegralArrayType(IntegralType<E> elementType, Class<T> arrayClass) {
        super(elementType, TypeToken.of(arrayClass));
        if (this.elementType.hasPrefix0x00())
            throw new RuntimeException("internal error");
        this.integralType = elementType;
    }

    @Override
    public T read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
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
        Preconditions.checkArgument(array != null, "null array");
        Preconditions.checkArgument(writer != null);
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            LongEncoder.write(writer, this.integralType.upCast(this.getArrayElement(array, i)));
        writer.writeByte(END);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        while (true) {
            final int first = reader.peek();
            if (first == END) {
                reader.skip(1);
                break;
            }
            reader.skip(LongEncoder.decodeLength(first));
        }
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.integralType.hasPrefix0xff();
    }
}

