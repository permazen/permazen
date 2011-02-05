
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

/**
 * Encodes {@code byte[]} arrays to and from hexadecimal strings.
 */
public final class ByteArrayEncoder {

    private ByteArrayEncoder() {
    }

    /**
     * Encode a {@code byte[]} array as a {@link String}.
     * Equivalent to:
     * <blockquote>
     * <code>encode(array, 0, array.length)</code>
     * </blockquote>
     *
     * @param array byte array, or {@code null}
     * @return hexadecimal string (or {@code null} if {@code array} was {@code null})
     */
    public static String encode(byte[] array) {
        return encode(array, 0, array.length);
    }

    /**
     * Encode a {@code byte[]} array as a {@link String}.
     *
     * @param array byte array, or {@code null}
     * @param off   offset into the array
     * @param len   number of bytes to convert
     * @return hexadecimal string (or {@code null} if {@code array} was {@code null})
     * @throws IndexOutOfBoundsException if array bounds are exceeded
     * @throws IllegalArgumentException  if {@code len} is greater than {@code Integer.MAX_VALUE / 2}
     */
    public static String encode(byte[] array, int off, int len) {

        // Check for null
        if (array == null)
            return null;

        // Check bounds
        if (off < 0 || len < 0 || off + len < 0 || off + len > array.length)
            throw new IndexOutOfBoundsException("array bounds exceeded");

        // Encode bytes
        if (len > Integer.MAX_VALUE / 2)
            throw new IllegalArgumentException("array is too long");
        char[] buf = new char[len * 2];
        for (int i = 0; i < len; i++) {
            int value = array[off + i] & 0xff;
            buf[i * 2] = Character.forDigit(value >> 4, 16);
            buf[i * 2 + 1] = Character.forDigit(value & 0xf, 16);
        }

        // Done
        return new String(buf);
    }

    /**
     * Decode a {@link String} back into a {@code byte[]} array.
     * Any extra whitespace in the string is ignored.
     *
     * @param text string previously encoded by {@link #encode}, or {@code null}
     * @return original {@code byte[]} array (or {@code null} if {@code text} was {@code null})
     * @throws IllegalArgumentException if any invalid non-whitespace characters are seen, or the number of hex digits is odd
     */
    public static byte[] decode(String text) {

        // Check for null
        if (text == null)
            return null;

        // Allocate array
        byte[] array = new byte[text.length() / 2];

        // Parse bytes
        int len = 0;
        boolean flipflop = false;
        int prevNibble = 0;
        final int limit = text.length();
        for (int pos = 0; pos < limit; pos++) {
            char ch = text.charAt(pos);
            if (Character.isWhitespace(ch))
                continue;
            int nibble = Character.digit(ch, 16);
            if (nibble == -1)
                throw new IllegalArgumentException("invalid character '" + ch + "' in byte array");
            if (flipflop)
                array[len++] = (byte)((prevNibble << 4) | nibble);
            else
                prevNibble = nibble;

            // bitwise inversion of boolean
            flipflop ^= true;
        }
        if (flipflop)
            throw new IllegalArgumentException("byte array has an odd number of digits");

        // Account for any squeezed-out whitespace
        if (len < array.length) {
            byte[] temp = new byte[len];
            System.arraycopy(array, 0, temp, 0, len);
            array = temp;
        }

        // Done
        return array;
    }
}

