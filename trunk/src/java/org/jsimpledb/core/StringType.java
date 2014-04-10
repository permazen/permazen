
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.string.ParseContext;
import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Non-null {@link String} type. Null values are not supported by this class.
 *
 * <p>
 * Strings are encoded as a sequence of characters followed by {@code 0x00}, where each character is encoded via
 * {@link UnsignedIntEncoder}, with the special exception that the characters {@code 0x0000} and {@code 0x0001}
 * are prefixed with a {@code 0x01} byte to avoid writing a {@code 0x00}. We rely on the fact that {@link UnsignedIntEncoder}
 * encodes {@code 0} and {@code 1} as {@code 0x00} and {@code 0x01}, respectively.
 * </p>
 */
class StringType extends FieldType<String> {

    private static final int END = 0x00;
    private static final int ESCAPE = 0x01;

    StringType() {
       super(String.class);
    }

    @Override
    public String read(ByteReader reader) {
        final StringBuilder buf = new StringBuilder();
        while (true) {
            int ch = UnsignedIntEncoder.read(reader);
            switch (ch) {
            case END:
                return buf.toString();
            case ESCAPE:
                final int ch2 = reader.readByte();
                if ((ch2 & ~1) != 0)
                    throw new IllegalArgumentException(String.format("invalid string escape sequence 0x%02x 0x%02x", ch, ch2));
                ch = ch2;
                break;
            default:
                if ((ch & ~0xffff) != 0)
                    throw new IllegalArgumentException(String.format("read out of range string character value 0x%08x", ch));
                break;
            }
            buf.append((char)ch);
        }
    }

    @Override
    public void write(ByteWriter writer, String value) {
        final int max = value.length();
        for (int i = 0; i < max; i++) {
            final int ch = value.charAt(i);
            switch (ch) {
            case END:
            case ESCAPE:
                writer.writeByte(ESCAPE);
                writer.writeByte(ch);
                break;
            default:
                UnsignedIntEncoder.write(writer, ch);
                break;
            }
        }
        writer.writeByte(END);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        int value = reader.readByte();
        while (true) {
            writer.writeByte(value);
            switch (value) {
            case END:
                return;
            case ESCAPE:
                writer.writeByte(reader.readByte());
                break;
            default:
                for (int length = UnsignedIntEncoder.decodeLength(value); --length > 0; )
                    writer.writeByte(reader.readByte());
                break;
            }
            value = reader.readByte();
        }
    }

    @Override
    public void skip(ByteReader reader) {
        int value = reader.readByte();
        while (true) {
            switch (value) {
            case END:
                return;
            case ESCAPE:
                reader.skip(1);
                break;
            default:
                reader.skip(UnsignedIntEncoder.decodeLength(value) - 1);
                break;
            }
            value = reader.readByte();
        }
    }

    @Override
    public byte[] getDefaultValue() {
        return new byte[] { (byte)END };
    }

    @Override
    public String toString(String value) {
        return StringEncoder.enquote(value);
    }

    @Override
    public String fromString(ParseContext ctx) {
        return StringEncoder.dequote(ctx.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group());
    }

    @Override
    protected boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }
}

