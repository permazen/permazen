
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.core.Encoding;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;
import io.permazen.util.UnsignedIntEncoder;

import org.dellroad.stuff.string.StringEncoder;

/**
 * Non-null {@link String} type. Null values are not supported by this class.
 *
 * <p>
 * Strings are encoded as a sequence of characters followed by {@code 0x00}, where each character is encoded via
 * {@link UnsignedIntEncoder}, with the special exception that the characters {@code 0x0000} and {@code 0x0001}
 * are prefixed with a {@code 0x01} byte to avoid writing a {@code 0x00}. We rely on the fact that {@link UnsignedIntEncoder}
 * encodes {@code 0} and {@code 1} as {@code 0x00} and {@code 0x01}, respectively.
 */
public class StringType extends BuiltinEncoding<String> {

    private static final long serialVersionUID = -7808183397158645337L;

    private static final int END = 0x00;
    private static final int ESCAPE = 0x01;

    public StringType() {
       super(String.class);
    }

    @Override
    public String read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
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
        Preconditions.checkArgument(writer != null);
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
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
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
    public String toString(String value) {
        Preconditions.checkArgument(value != null, "null value");
        return StringEncoder.encode(value, false);
    }

    @Override
    public String fromString(String string) {
        return StringEncoder.decode(string);
    }

    @Override
    public String toParseableString(String value) {
        return StringEncoder.enquote(value);
    }

    @Override
    public String fromParseableString(ParseContext ctx) {
        return StringEncoder.dequote(ctx.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group());
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

// Conversion

    @Override
    public <S> String convert(Encoding<S> type, S value) {

        // Handle null
        if (value == null)
            return null;

        // Unwrap nullable types
        if (type instanceof NullSafeType)
            type = ((NullSafeType<S>)type).inner;

        // Special case for character
        if (value instanceof Character)
            return new String(new char[] { (Character)value });

        // Special case for character array
        if (type instanceof CharacterArrayType)
            return new String((char[])value);

        // Defer to superclass
        return super.convert(type, value);
    }
}
