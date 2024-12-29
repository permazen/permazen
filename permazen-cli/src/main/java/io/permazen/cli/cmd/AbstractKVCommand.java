
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.Parser;
import io.permazen.util.ByteData;

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
     * Convert {@code byte[]} data into a double-quoted C-string representation, surrounded by double quotes,
     * with non-ASCII bytes, double-quotes, and backslashes escaped with a backslash.
     *
     * <p>
     * Supported escapes are {@code \\}, {@code \"}, {@code \b}, {@code \f}, {@code \t}, {@code \n}, {@code \r}, and {@code \xNN}.
     *
     * @param data byte array
     * @return C string representation
     */
    public static String toCString(ByteData data) {
        Preconditions.checkArgument(data != null, "null data");
        StringBuilder buf = new StringBuilder(data.size() + 4);
        buf.append('"');
        for (byte b : data.toByteArray()) {
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
     * Parse {@code byte[]} data encoded as a double-quoted C-string representation by {@link #toCString toCString()}.
     *
     * @param string C string
     * @return byte data
     * @throws IllegalArgumentException if {@code string} is malformed
     */
    public static ByteData fromCString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        Preconditions.checkArgument(string.length() >= 2 && string.charAt(0) == '"' && string.charAt(string.length() - 1) == '"',
            "string is not contained in double quotes");
        string = string.substring(1, string.length() - 1);
        final ByteData.Writer buf = ByteData.newWriter(string.length());
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);

            // Handle unescaped characters
            if (ch != '\\') {
                Preconditions.checkArgument(ch >= 0x20 && ch <= 0x7e,
                  String.format("illegal character 0x%02x in encoded string", ch & 0xff));
                buf.write(ch);
                continue;
            }

            // Get next char
            Preconditions.checkArgument(++i < string.length(), "illegal trailing '\\' in encoded string");
            ch = string.charAt(i);

            // Check for special escapes
            switch (ch) {
            case '"':
                buf.write('"');
                continue;
            case '\\':
                buf.write('\\');
                continue;
            case 'b':
                buf.write('\b');
                continue;
            case 't':
                buf.write('\t');
                continue;
            case 'n':
                buf.write('\n');
                continue;
            case 'f':
                buf.write('\f');
                continue;
            case 'r':
                buf.write('\r');
                continue;
            default:
                break;
            }

            // Must be hex escape
            if (ch != 'x')
                throw new IllegalArgumentException(String.format("illegal escape sequence '\\%c' in encoded string", ch));

            // Decode hex value
            Preconditions.checkArgument(i + 2 < string.length(), "illegal truncated '\\x' escape sequence in encoded string");
            int value = 0;
            for (int j = 0; j < 2; j++) {
                int nibble = Character.digit(string.charAt(++i), 16);
                if (nibble == -1) {
                    throw new IllegalArgumentException(String.format(
                      "illegal escape sequence '%s' in encoded string", string.substring(i - j - 2, i - j + 2)));
                }
                assert nibble >= 0 && nibble <= 0xf;
                value = (value << 4) | nibble;
            }

            // Append decoded byte
            buf.write(value);
        }
        return buf.toByteData();
    }

// BytesParser

    /**
     * Parses {@code byte[]} data as hexadecimal or doubly-quoted "C" style string.
     *
     * @see AbstractKVCommand#toCString
     */
    public static class BytesParser implements Parser<ByteData> {

        @Override
        public ByteData parse(Session session, String text) {
            Preconditions.checkArgument(session != null, "null session");
            Preconditions.checkArgument(text != null, "null text");
            try {
                return AbstractKVCommand.fromCString(text);
            } catch (IllegalArgumentException e) {
                // failed
            }
            try {
                return ByteData.fromHex(text);
            } catch (IllegalArgumentException e) {
                // failed
            }
            throw new IllegalArgumentException("invalid byte array value");
        }
    }

// KVAction

    public interface KVAction extends Session.RetryableTransactionalAction {

        @Override
        default SessionMode getTransactionMode(Session session) {
            return SessionMode.KEY_VALUE;
        }
    }
}
