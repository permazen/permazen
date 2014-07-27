
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link Character} type.
 */
class CharacterType extends PrimitiveType<Character> {

    private static final byte[] DEFAULT_VALUE = new byte[2];

    CharacterType() {
       super(Primitive.CHARACTER);
    }

    @Override
    public Character read(ByteReader reader) {
        final int hi = reader.readByte();
        final int lo = reader.readByte();
        return (char)((hi << 8) | lo);
    }

    @Override
    public void write(ByteWriter writer, Character value) {
        final int hi = (int)value >> 8;
        final int lo = (int)value & 0xff;
        writer.writeByte(hi);
        writer.writeByte(lo);
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(2);
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public String toString(Character value) {
        return StringEncoder.enquote(String.valueOf(value));
    }

    @Override
    public Character fromString(ParseContext context) {
        final String s = context.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group();
        if (s.length() != 3)
            throw new IllegalArgumentException("more than one character found within quotation marks");
        return s.charAt(1);
    }
}

