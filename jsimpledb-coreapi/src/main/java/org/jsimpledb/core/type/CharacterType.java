
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.ParseContext;

/**
 * {@link Character} type.
 */
public class CharacterType extends PrimitiveType<Character> {

    private static final long serialVersionUID = -3328818464598650353L;

    public CharacterType() {
       super(Primitive.CHARACTER);
    }

    @Override
    public Character read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        final int hi = reader.readByte();
        final int lo = reader.readByte();
        return (char)((hi << 8) | lo);
    }

    @Override
    public void write(ByteWriter writer, Character value) {
        Preconditions.checkArgument(writer != null);
        final int hi = (int)value >> 8;
        final int lo = (int)value & 0xff;
        writer.writeByte(hi);
        writer.writeByte(lo);
    }

    @Override
    public void skip(ByteReader reader) {
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
        final String s = StringEncoder.decode(string);
        if (s.length() != 1)
            throw new IllegalArgumentException("more than one character found");
        return s.charAt(0);
    }

    @Override
    public String toParseableString(Character value) {
        return StringEncoder.enquote(String.valueOf(value));
    }

    @Override
    public Character fromParseableString(ParseContext context) {
        final String s = StringEncoder.dequote(context.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group());
        if (s.length() != 1)
            throw new IllegalArgumentException("more than one character found within quotation marks");
        return s.charAt(0);
    }

// Conversion

    @Override
    public <S> Character convert(FieldType<S> type, S value) {

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

