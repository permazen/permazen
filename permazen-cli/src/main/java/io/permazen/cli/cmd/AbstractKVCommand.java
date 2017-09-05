
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.util.ByteUtil;
import io.permazen.util.ParseContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractKVCommand extends AbstractCommand {

    /**
     * Matches the doubly-quoted C strings returnd by {@link #toCString toCString()}.
     */
    public static final Pattern CSTRING_PATTERN = Pattern.compile(
      "\"([\\x20\\x21\\x23-\\x5b\\x5d-\\x7e]|\\\\([\\\\bftrn\"]|x[\\p{XDigit}]{2}))*\"");

    /**
     * Matches hexadecimal byte strings.
     */
    public static final Pattern HEXBYTES_PATTERN = Pattern.compile("([\\p{XDigit}]{2}){1,}");

    protected AbstractKVCommand(String spec) {
        super(spec);
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        if ("bytes".equals(typeName))
            return new BytesParser();
        return super.getParser(typeName);
    }

    /**
     * Convert a {@code byte[]} array into a double-quoted C-string representation, surrounded by double quotes,
     * with non-ASCII bytes, double-quotes, and backslashes escaped with a backslash.
     *
     * <p>
     * Supported escapes are {@code \\}, {@code \"}, {@code \b}, {@code \f}, {@code \t}, {@code \n}, {@code \r}, and {@code \xNN}.
     *
     * @param data byte array
     * @return C string representation
     */
    public static String toCString(byte[] data) {
        Preconditions.checkArgument(data != null, "null data");
        StringBuilder buf = new StringBuilder(data.length + 4);
        buf.append('"');
        for (byte b : data) {
            final int ch = b & 0xff;

            // Handle special escapes
            switch (ch) {
            case '\\':
            case '"':
                buf.append('\\').append((char)ch);
                continue;
            case '\b':
                buf.append('\\').append('b');
                continue;
            case '\f':
                buf.append('\\').append('f');
                continue;
            case '\t':
                buf.append('\\').append('t');
                continue;
            case '\n':
                buf.append('\\').append('n');
                continue;
            case '\r':
                buf.append('\\').append('r');
                continue;
            default:
                break;
            }

            // Handle printables
            if (ch >= 0x20 && ch <= 0x7e) {
                buf.append((char)ch);
                continue;
            }

            // Escape it using 2 digit hex
            buf.append('\\');
            buf.append('x');
            buf.append(Character.forDigit(ch >> 4, 16));
            buf.append(Character.forDigit(ch & 0x0f, 16));
        }
        buf.append('"');
        return buf.toString();
    }

    /**
     * Parse a {@code byte[]} array encoded as a double-quoted C-string representation by {@link #toCString toCString()}.
     *
     * @param string C string
     * @return byte array
     * @throws IllegalArgumentException if {@code string} is malformed
     */
    public static byte[] fromCString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        Preconditions.checkArgument(string.length() >= 2 && string.charAt(0) == '"' && string.charAt(string.length() - 1) == '"',
            "string is not contained in double quotes");
        string = string.substring(1, string.length() - 1);
        byte[] buf = new byte[string.length()];
        int index = 0;
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);

            // Handle unescaped characters
            if (ch != '\\') {
                Preconditions.checkArgument(ch >= 0x20 && ch <= 0x7e,
                  String.format("illegal character 0x%02x in encoded string", ch & 0xff));
                buf[index++] = (byte)ch;
                continue;
            }

            // Get next char
            Preconditions.checkArgument(++i < string.length(), "illegal trailing '\\' in encoded string");
            ch = string.charAt(i);

            // Check for special escapes
            switch (ch) {
            case '"':
                buf[index++] = (byte)'"';
                continue;
            case '\\':
                buf[index++] = (byte)'\\';
                continue;
            case 'b':
                buf[index++] = (byte)'\b';
                continue;
            case 't':
                buf[index++] = (byte)'\t';
                continue;
            case 'n':
                buf[index++] = (byte)'\n';
                continue;
            case 'f':
                buf[index++] = (byte)'\f';
                continue;
            case 'r':
                buf[index++] = (byte)'\r';
                continue;
            default:
                break;
            }

            // Must be hex escape
            Preconditions.checkArgument(ch == 'x', "illegal escape sequence '\\" + ch + "' in encoded string");

            // Decode hex value
            Preconditions.checkArgument(i + 2 < string.length(), "illegal truncated '\\x' escape sequence in encoded string");
            int value = 0;
            for (int j = 0; j < 2; j++) {
                int nibble = Character.digit(string.charAt(++i), 16);
                Preconditions.checkArgument(nibble != -1,
                  "illegal escape sequence '" + string.substring(i - j - 2, i - j + 2) + "' in encoded string");
                assert nibble >= 0 && nibble <= 0xf;
                value = (value << 4) | nibble;
            }

            // Append decoded byte
            buf[index++] = (byte)value;
        }
        if (index < buf.length) {
            final byte[] newbuf = new byte[index];
            System.arraycopy(buf, 0, newbuf, 0, index);
            buf = newbuf;
        }
        return buf;
    }

// BytesParser

    private static class BytesParser implements Parser<byte[]> {

        @Override
        public byte[] parse(ParseSession session, ParseContext ctx, boolean complete) {
            final Matcher cstringMatcher = ctx.tryPattern(CSTRING_PATTERN);
            if (cstringMatcher != null)
                return AbstractKVCommand.fromCString(cstringMatcher.group());
            final Matcher hexbytesMatcher = ctx.tryPattern(HEXBYTES_PATTERN);
            if (hexbytesMatcher != null)
                return ByteUtil.parse(hexbytesMatcher.group());
            throw new ParseException(ctx, "invalid byte array value");
        }
    }
}

