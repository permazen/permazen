
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Support superclass for non-null object array encodings.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 * However the array elements can be null.
 *
 * <p>
 * In the binary encoding, array elements are simply concatenated, with each element preceded by a {@code 0x01} byte
 * unless encoded array elements never start with {@code 0x00}. After the last array element, a final {@code 0x00}
 * is then appended. This encoding ensures lexicographic ordering (though there is a subtlety here, which is that when
 * omitting the {@code 0x01} bytes, we are relying on the fact that for any {@link Encoding} no encoded value can be a
 * prefix of a another value, because that violates the self-delimiting requirement; as a result, it's not possible
 * for two concatenated values [B,C] to sort before a single value [A] if A &lt; B).
 *
 * @param <E> array element type
 */
public class ObjectArrayEncoding<E> extends ArrayEncoding<E[], E> {

    private static final long serialVersionUID = -2337331922923184256L;

    private static final int END = 0x00;
    private static final int VALUE = 0x01;

    private final boolean inlineValue;

    @SuppressWarnings("serial")
    public ObjectArrayEncoding(Encoding<E> elementEncoding) {
        super(elementEncoding, new TypeToken<E[]>() { }.where(new TypeParameter<E>() { }, elementEncoding.getTypeToken()));
        Preconditions.checkArgument(!elementEncoding.getTypeToken().isPrimitive(), "illegal primitive element type");
        this.inlineValue = !elementEncoding.hasPrefix0x00();
    }

    @Override
    public E[] read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final ArrayList<E> list = new ArrayList<>();
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inlineValue)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException(String.format("invalid encoding of %s", this));
            list.add(this.elementEncoding.read(reader));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteData.Writer writer, E[] array) {
        Preconditions.checkArgument(writer != null);
        for (E obj : array) {
            if (!this.inlineValue)
                writer.write(VALUE);
            this.elementEncoding.write(writer, obj);
        }
        writer.write(END);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inlineValue)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException(String.format("invalid encoding of %s", this));
            this.elementEncoding.skip(reader);
        }
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.inlineValue && this.elementEncoding.hasPrefix0xff();
    }

// ArrayEncoding

    @Override
    protected int getArrayLength(E[] array) {
        return array.length;
    }

    @Override
    protected E getArrayElement(E[] array, int index) {
        return array[index];
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E[] createArray(List<E> elements) {
        return elements.toArray((E[])Array.newInstance(this.elementEncoding.getTypeToken().getRawType(), elements.size()));
    }
}
