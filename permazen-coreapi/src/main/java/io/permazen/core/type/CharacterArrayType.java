
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Chars;
import com.google.common.reflect.TypeToken;

import io.permazen.core.FieldType;
import io.permazen.core.FieldTypeRegistry;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.List;

/**
 * {@code char[]} array type. Does not support null arrays.
 *
 * <p>
 * We use the same encoding as {@link StringType}.
 */
public class CharacterArrayType extends ArrayType<char[], Character> {

    private static final long serialVersionUID = 968583366001367828L;

    private final StringType stringType = new StringType();

    @SuppressWarnings("serial")
    public CharacterArrayType() {
        super(FieldTypeRegistry.CHARACTER, new TypeToken<char[]>() { });
    }

    @Override
    public char[] read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return this.stringType.read(reader).toCharArray();
    }

    @Override
    public void write(ByteWriter writer, char[] array) {
        Preconditions.checkArgument(writer != null);
        this.stringType.write(writer, new String(array));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
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

// Conversion

    @Override
    public <S> char[] convert(FieldType<S> type, S value) {

        // Special case for String
        if (value instanceof String)
            return ((String)value).toCharArray();

        // Defer to superclass
        return super.convert(type, value);
    }
}

