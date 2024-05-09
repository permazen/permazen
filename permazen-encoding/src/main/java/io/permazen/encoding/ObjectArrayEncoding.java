
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Array type for object arrays having non-primitive element types. Does not support null arrays.
 *
 * <p>
 * In the binary encoding, array elements are simply concatenated, with each element preceded by a {@code 0x01} byte.
 * After the last element, a final {@code 0x00} byte follows. This encoding ensures lexicographic ordering.
 *
 * @param <E> array element type
 */
public class ObjectArrayEncoding<E> extends ArrayEncoding<E[], E> {

    private static final long serialVersionUID = -2337331922923184256L;

    private static final int END = 0x00;
    private static final int VALUE = 0x01;

    private final boolean inline;

    @SuppressWarnings("serial")
    public ObjectArrayEncoding(EncodingId encodingId, Encoding<E> elementEncoding) {
        super(encodingId, elementEncoding,
          new TypeToken<E[]>() { }.where(new TypeParameter<E>() { }, elementEncoding.getTypeToken()));
        Preconditions.checkArgument(!elementEncoding.getTypeToken().isPrimitive(), "illegal primitive element type");
        this.inline = !elementEncoding.hasPrefix0x00();
    }

    @Override
    public E[] read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        final ArrayList<E> list = new ArrayList<>();
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inline)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException("invalid encoding of " + this);
            list.add(this.elementEncoding.read(reader));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteWriter writer, E[] array) {
        Preconditions.checkArgument(writer != null);
        for (E obj : array) {
            if (!this.inline)
                writer.writeByte(VALUE);
            this.elementEncoding.write(writer, obj);
        }
        writer.writeByte(END);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inline)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException("invalid encoding of " + this);
            this.elementEncoding.skip(reader);
        }
    }

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
