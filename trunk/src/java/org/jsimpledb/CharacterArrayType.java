
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.primitives.Chars;
import com.google.common.reflect.TypeToken;

import java.util.List;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@code char[]} array type. Does not support null arrays.
 *
 * <p>
 * We use the same encoding as {@link StringType}.
 * </p>
 */
class CharacterArrayType extends ArrayType<char[], Character> {

    private final StringType stringType = new StringType();

    @SuppressWarnings("serial")
    CharacterArrayType() {
        super(FieldType.CHARACTER, new TypeToken<char[]>() { });
    }

    @Override
    public char[] read(ByteReader reader) {
        return this.stringType.read(reader).toCharArray();
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        this.stringType.copy(reader, writer);
    }

    @Override
    public void write(ByteWriter writer, char[] array) {
        this.stringType.write(writer, new String(array));
    }

    @Override
    public void skip(ByteReader reader) {
        this.stringType.skip(reader);
    }

    @Override
    public byte[] getDefaultValue() {
        return this.stringType.getDefaultValue();
    }

    @Override
    protected boolean hasPrefix0x00() {
        return this.stringType.hasPrefix0x00();
    }

    @Override
    protected boolean hasPrefix0xff() {
        return this.stringType.hasPrefix0xff();
    }

    @Override
    protected int getArrayLength(char[] array) {
        return array.length;
    }

    @Override
    protected Character getArrayElement(char[] array, int index) {
        return array[index];
    }

    @Override
    protected char[] createArray(List<Character> elements) {
        return Chars.toArray(elements);
    }
}

