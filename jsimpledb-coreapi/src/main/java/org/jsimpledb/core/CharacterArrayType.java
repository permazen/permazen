
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

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
 */
class CharacterArrayType extends ArrayType<char[], Character> {

    private final StringType stringType = new StringType();

    @SuppressWarnings("serial")
    CharacterArrayType() {
        super(FieldTypeRegistry.CHARACTER, new TypeToken<char[]>() { });
    }

    @Override
    public char[] read(ByteReader reader) {
        return this.stringType.read(reader).toCharArray();
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
    public boolean hasPrefix0x00() {
        return this.stringType.hasPrefix0x00();
    }

    @Override
    public boolean hasPrefix0xff() {
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

