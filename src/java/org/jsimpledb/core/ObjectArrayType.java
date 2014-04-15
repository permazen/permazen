
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Array type for object arrays having non-primitive element types. Does not support null arrays.
 *
 * @param <E> array element type
 */
class ObjectArrayType<E> extends ArrayType<E[], E> {

    private static final int END = 0x00;
    private static final int VALUE = 0x01;

    private final boolean inline;

    @SuppressWarnings("serial")
    ObjectArrayType(FieldType<E> elementType) {
        super(elementType, new TypeToken<E[]>() { }.where(new TypeParameter<E>() { }, elementType.typeToken));
        if (elementType.typeToken.isPrimitive())
            throw new IllegalArgumentException("illegal primitive element type " + elementType);
        this.inline = !elementType.hasPrefix0x00();
    }

    @Override
    public E[] read(ByteReader reader) {
        final ArrayList<E> list = new ArrayList<>();
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inline)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException("invalid encoding of " + this);
            list.add(this.elementType.read(reader));
        }
        return this.createArray(list);
    }

    @Override
    public void write(ByteWriter writer, E[] array) {
        for (int i = 0; i < array.length; i++) {
            if (!this.inline)
                writer.writeByte(VALUE);
            this.elementType.write(writer, array[i]);
        }
        writer.writeByte(END);
    }

    @Override
    public void skip(ByteReader reader) {
        while (true) {
            final int first = reader.readByte();
            if (first == END)
                break;
            if (this.inline)
                reader.unread();
            else if (first != VALUE)
                throw new IllegalArgumentException("invalid encoding of " + this);
            this.elementType.skip(reader);
        }
    }

    @Override
    public byte[] getDefaultValue() {
        return new byte[] { (byte)END };
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
        return elements.toArray((E[])Array.newInstance(this.elementType.typeToken.getRawType(), elements.size()));
    }
}

