
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;

import java.util.Comparator;

/**
 * Byte manipulation utilities.
 */
public final class ByteUtil {

    /**
     * An empty byte array. This is the minimum value according to {@link #COMPARATOR}.
     */
    public static final byte[] EMPTY = new byte[0];

    /**
     * {@link Comparator} that compares two byte arrays lexicographically using unsigned values.
     */
    public static final Comparator<byte[]> COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] b1, byte[] b2) {
            return ByteUtil.compare(b1, b2);
        }
    };

    /**
     * A {@link Converter} that converts between {@code byte[]} arrays and hexadecimal {@link String}s.
     */
    public static final Converter<byte[], String> STRING_CONVERTER = new Converter<byte[], String>() {

        @Override
        public String doForward(byte[] b) {
            return b != null ? ByteUtil.toString(b) : null;
        }

        @Override
        public byte[] doBackward(String s) {
            return s != null ? ByteUtil.parse(s) : null;
        }
    };

    private ByteUtil() {
    }

    /**
     * Compare two byte arrays lexicographically using unsigned values.
     *
     * @param b1 first byte array
     * @param b2 second byte array
     * @return -1 if {@code b1 < b2}, 1 if {@code b1 > b2}, or zero if {@code b1 = b2}
     * @throws NullPointerException if {@code b1} or {@code b2} is null
     */
    public static int compare(byte[] b1, byte[] b2) {
        if (b1 == b2)
            return 0;
        final int sharedLength = Math.min(b1.length, b2.length);
        for (int i = 0; i < sharedLength; i++) {
            final int v1 = b1[i] & 0xff;
            final int v2 = b2[i] & 0xff;
            if (v1 < v2)
                return -1;
            if (v1 > v2)
                return 1;
        }
        if (b1.length < b2.length)
            return -1;
        if (b1.length > b2.length)
            return 1;
        return 0;
    }

    /**
     * Determine the smaller of two byte arrays when compared lexicographically using unsigned values.
     *
     * @param b1 first byte array
     * @param b2 second byte array
     * @return {@code b1} if {@code b1 <= b2}, otherwise {@code b2}
     * @throws NullPointerException if {@code b1} or {@code b2} is null
     */
    public static byte[] min(byte[] b1, byte[] b2) {
        return ByteUtil.compare(b1, b2) <= 0 ? b1 : b2;
    }

    /**
     * Determine the larger of two byte arrays when compared lexicographically using unsigned values.
     *
     * @param b1 first byte array
     * @param b2 second byte array
     * @return {@code b1} if {@code b1 >= b2}, otherwise {@code b2}
     * @throws NullPointerException if {@code b1} or {@code b2} is null
     */
    public static byte[] max(byte[] b1, byte[] b2) {
        return ByteUtil.compare(b1, b2) >= 0 ? b1 : b2;
    }

    /**
     * Determine if the first of two {@code byte[]} arrays is a prefix of the second.
     *
     * @param prefix prefix to check
     * @param value value to check for having {@code prefix} as a prefix
     * @return true if {@code prefix} is a prefix of {@code value}
     * @throws NullPointerException if {@code prefix} or {@code value} is null
     */
    public static boolean isPrefixOf(byte[] prefix, byte[] value) {
        if (prefix.length > value.length)
            return false;
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i])
                return false;
        }
        return true;
    }

    /**
     * Get the next key greater than the given key in unsigned lexicographic ordering.
     * This creates a new key simply by appending a {@code 0x00} byte to the data
     * contained in the given key.
     *
     * @param key previous key
     * @return next key after {@code key}
     * @throws NullPointerException if {@code key} is null
     */
    public static byte[] getNextKey(byte[] key) {
        final byte[] nextKey = new byte[key.length + 1];
        System.arraycopy(key, 0, nextKey, 0, key.length);
        return nextKey;
    }

    /**
     * Determine whether {@code key2} is the next key after {@code key1}.
     *
     * @param key1 first key
     * @param key2 second key
     * @return true if {@code key2} immediately follows {@code key1}
     * @throws NullPointerException if either parameter is null
     */
    public static boolean isConsecutive(byte[] key1, byte[] key2) {
        if (key2.length != key1.length + 1)
            return false;
        if (key2[key1.length] != 0)
            return false;
        for (int i = 0; i < key1.length; i++) {
            if (key1[i] != key2[i])
                return false;
        }
        return true;
    }

    /**
     * Get the first key that would be greater than the given key in unsigned lexicographic
     * ordering <i>and</i> that does not have the given key as a prefix.
     *
     * @param prefix lower bound prefix key
     * @return next key not having {@code prefix} as a prefix
     * @throws IllegalArgumentException if {@code prefix} has zero length
     * @throws IllegalArgumentException if {@code prefix} contains only {@code 0xff} bytes
     * @throws NullPointerException if {@code prefix} is null
     */
    public static byte[] getKeyAfterPrefix(byte[] prefix) {
        int len = prefix.length;
        if (len == 0)
            throw new IllegalArgumentException("empty prefix");
        while (len > 0 && prefix[len - 1] == (byte)0xff)
            len--;
        if (len <= 0)
            throw new IllegalArgumentException("prefix contains only 0xff bytes");
        final byte[] buf = new byte[len];
        System.arraycopy(prefix, 0, buf, 0, len);
        buf[len - 1]++;
        return buf;
    }

    /**
     * Convert a byte array into a string of hex digits, or {@code "null"} if {@code buf} is null.
     *
     * @param buf bytes
     * @return string encoding of {@code buf}
     * @see #parse parse()
     */
    public static String toString(byte[] buf) {
        if (buf == null)
            return "null";
        final char[] result = new char[buf.length * 2];
        int off = 0;
        for (int i = 0; i < buf.length; i++) {
            int value = buf[i];
            result[off++] = Character.forDigit((value >> 4) & 0x0f, 16);
            result[off++] = Character.forDigit(value & 0x0f, 16);
        }
        return new String(result);
    }

    /**
     * Decode a hexadecimal {@link String} into a {@code byte[]} array. The string must have an even
     * number of digits and contain no other characters (e.g., whitespace).
     *
     * @param text string previously encoded by {@link #toString(byte[])}
     * @return {@code byte[]} decoding of {@code text}
     * @throws IllegalArgumentException if any non-hexadecimal characters are found or the number of characters is odd
     * @throws NullPointerException if {@code text} is null
     * @see #toString(byte[]) toString()
     */
    public static byte[] parse(String text) {
        if ((text.length() & 1) != 0)
            throw new IllegalArgumentException("byte array has an odd number of digits");
        final byte[] array = new byte[text.length() / 2];
        int pos = 0;
        for (int i = 0; pos < text.length(); i++) {
            final int nib1 = Character.digit(text.charAt(pos++), 16);
            if (nib1 == -1)
                throw new IllegalArgumentException("invalid hex digit `" + text.charAt(pos - 1) + "'");
            final int nib2 = Character.digit(text.charAt(pos++), 16);
            if (nib2 == -1)
                throw new IllegalArgumentException("invalid hex digit `" + text.charAt(pos - 1) + "'");
            array[i] = (byte)((nib1 << 4) | nib2);
        }
        return array;
    }

    /**
     * Read an {@code int} as four big-endian bytes.
     *
     * @param reader input
     * @return decoded integer
     * @throws IndexOutOfBoundsException if less than four bytes remain in {@code reader}
     * @throws NullPointerException if {@code reader} is null
     * @see #writeInt writeInt()
     */
    public static int readInt(ByteReader reader) {
        return (reader.readByte() << 24) | (reader.readByte() << 16) | (reader.readByte() << 8) | reader.readByte();
    }

    /**
     * Write an {@code int} as four big-endian bytes.
     *
     * @param writer byte destination
     * @param value value to write
     * @see #readInt readInt()
     * @throws NullPointerException if {@code writer} is null
     */
    public static void writeInt(ByteWriter writer, int value) {
        writer.writeByte(value >> 24);
        writer.writeByte(value >> 16);
        writer.writeByte(value >> 8);
        writer.writeByte(value);
    }

    /**
     * Read a {@code long} as eight big-endian bytes.
     *
     * @param reader input
     * @return decoded long
     * @throws IndexOutOfBoundsException if less than eight bytes remain in {@code reader}
     * @see #writeLong writeLong()
     */
    public static long readLong(ByteReader reader) {
        return
            ((long)reader.readByte() << 56) | ((long)reader.readByte() << 48)
          | ((long)reader.readByte() << 40) | ((long)reader.readByte() << 32)
          | ((long)reader.readByte() << 24) | ((long)reader.readByte() << 16)
          | ((long)reader.readByte() <<  8) |  (long)reader.readByte();
    }

    /**
     * Write a {@code long} as eight big-endian bytes.
     *
     * @param writer byte destination
     * @param value value to write
     * @see #readLong readLong()
     * @throws NullPointerException if {@code writer} is null
     */
    public static void writeLong(ByteWriter writer, long value) {
        writer.writeByte((int)(value >> 56));
        writer.writeByte((int)(value >> 48));
        writer.writeByte((int)(value >> 40));
        writer.writeByte((int)(value >> 32));
        writer.writeByte((int)(value >> 24));
        writer.writeByte((int)(value >> 16));
        writer.writeByte((int)(value >> 8));
        writer.writeByte((int)value);
    }
}

