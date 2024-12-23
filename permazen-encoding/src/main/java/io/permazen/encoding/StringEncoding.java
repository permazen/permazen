
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.OptionalInt;

import org.dellroad.stuff.string.StringEncoder;

/**
 * Non-null {@link String} encoding.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Strings are encoded as a sequence of characters followed by {@code 0x00}, where each character is encoded via
 * {@link UnsignedIntEncoder}, with the special exception that the characters {@code 0x0000} and {@code 0x0001}
 * are prefixed with a {@code 0x01} byte to avoid writing a {@code 0x00}. We rely on the fact that {@link UnsignedIntEncoder}
 * encodes {@code 0} and {@code 1} as {@code 0x00} and {@code 0x01}, respectively. As a result of this encoding,
 * this encoding {@link #sortsNaturally}.
 */
public class StringEncoding extends AbstractEncoding<String> {

    private static final long serialVersionUID = -7808183397158645337L;

    private static final int END = 0x00;
    private static final int ESCAPE = 0x01;

    public StringEncoding() {
       super(String.class);
    }

    @Override
    public String read(ByteData.Reader reader) {
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
    public void write(ByteData.Writer writer, String value) {
        Preconditions.checkArgument(writer != null);
        final int max = value.length();
        for (int i = 0; i < max; i++) {
            final int ch = value.charAt(i);
            switch (ch) {
            case END:
            case ESCAPE:
                writer.write(ESCAPE);
                writer.write(ch);
                break;
            default:
                UnsignedIntEncoder.write(writer, ch);
                break;
            }
        }
        writer.write(END);
    }

    @Override
    public void skip(ByteData.Reader reader) {
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

    /**
     * Encode the given non-null value as a {@link String}.
     *
     * <p>
     * Because {@link Encoding#toString Encoding.toString()} disallows XML-invalid characters, the returned
     * string is not always equal to {@code value}. Instead, the implementation in {@link StringEncoding}
     * delegates to {@link StringEncoder#encode StringEncoder.encode()} to backslash-escape invalid characters.
     *
     * @param value string value, never null
     * @return backslash-escaped {@code value}
     * @throws IllegalArgumentException if {@code value} is null
     */
    @Override
    public String toString(String value) {
        Preconditions.checkArgument(value != null, "null value");
        return StringEncoder.encode(value, false);
    }

    /**
     * Parse a non-null value previously encoded by {@link #toString(String) toString()}.
     *
     * <p>
     * The implementation in {@link StringEncoding} delegates to {@link StringEncoder#decode StringEncoder.decode()}.
     *
     * @param string non-null value previously encoded by {@link #toString(String) toString()}
     * @return decoded value
     * @throws IllegalArgumentException if {@code string} is invalid
     * @throws IllegalArgumentException if {@code string} is null
     */
    @Override
    public String fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return StringEncoder.decode(string);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
    }

// Conversion

    @Override
    public <S> String convert(Encoding<S> type, S value) {

        // Handle null
        if (value == null)
            return null;

        // Unwrap nullable types
        if (type instanceof NullSafeEncoding)
            type = ((NullSafeEncoding<S>)type).inner;

        // Special case for character
        if (value instanceof Character)
            return new String(new char[] { (Character)value });

        // Special case for character array
        if (type instanceof CharacterArrayEncoding)
            return new String((char[])value);

        // Defer to superclass
        return super.convert(type, value);
    }
}
