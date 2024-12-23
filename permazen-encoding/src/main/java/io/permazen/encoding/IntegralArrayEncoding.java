
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.util.ArrayList;

/**
 * Support superclass for non-null integral primitive array encodings.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Arrays are encoded as a sequence of numerical values followed by {@code 0x00}, where each value is encoded via
 * {@link LongEncoder}. Note, we take advantage of the fact that {@link LongEncoder} does not emit values starting
 * with {@code 0x00}.
 */
public abstract class IntegralArrayEncoding<T, E extends Number> extends Base64ArrayEncoding<T, E> {

    private static final long serialVersionUID = -5185297639150351646L;

    private static final int END = 0x00;

    @SuppressWarnings("serial")
    protected IntegralArrayEncoding(IntegralEncoding<E> elementEncoding, Class<T> arrayClass) {
        super(elementEncoding, TypeToken.of(arrayClass));
        if (this.elementEncoding.hasPrefix0x00())
            throw new RuntimeException("internal error");
    }

    @Override
    public T read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final ArrayList<E> list = new ArrayList<>();
        while (true) {
            final int first = reader.peek();
            if (first == END) {
                reader.skip(1);
                break;
            }
            list.add(this.getIntegralEncoding().downCast(LongEncoder.read(reader)));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteData.Writer writer, T array) {
        Preconditions.checkArgument(array != null, "null array");
        Preconditions.checkArgument(writer != null);
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++)
            LongEncoder.write(writer, this.getIntegralEncoding().upCast(this.getArrayElement(array, i)));
        writer.write(END);
    }

    @Override
    public void skip(ByteData.Reader reader) {
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
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.elementEncoding.hasPrefix0xff();
    }

    private IntegralEncoding<E> getIntegralEncoding() {
        return (IntegralEncoding<E>)this.elementEncoding;
    }
}
