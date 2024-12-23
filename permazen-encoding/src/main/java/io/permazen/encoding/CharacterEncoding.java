
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.string.StringEncoder;

/**
 * {@link Character} type.
 */
public class CharacterEncoding extends PrimitiveEncoding<Character> {

    private static final long serialVersionUID = -3328818464598650353L;

    public CharacterEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.CHARACTER);
    }

    @Override
    public Character read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final int hi = reader.readByte();
        final int lo = reader.readByte();
        return (char)((hi << 8) | lo);
    }

    @Override
    public void write(ByteData.Writer writer, Character value) {
        Preconditions.checkArgument(writer != null);
        final int hi = (int)value >> 8;
        final int lo = (int)value & 0xff;
        writer.write(hi);
        writer.write(lo);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(2);
    }

    @Override
    public String toString(Character value) {
        Preconditions.checkArgument(value != null, "null value");
        return StringEncoder.encode(String.valueOf(value), true);
    }

    @Override
    public Character fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        final String s = StringEncoder.decode(string);
        if (s.length() != 1)
            throw new IllegalArgumentException("more than one character found");
        return s.charAt(0);
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.of(2);
    }

// Conversion

    @Override
    public <S> Character convert(Encoding<S> type, S value) {

        // Special case for a string of length one
        if (value instanceof String && ((String)value).length() == 1)
            return ((String)value).charAt(0);

        // Defer to superclass
        return super.convert(type, value);
    }

    @Override
    protected Character convertNumber(Number value) {
        return (char)value.intValue();
    }
}
