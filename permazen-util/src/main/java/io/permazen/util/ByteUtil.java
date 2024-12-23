
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * {@link ByteData} manipulation utilities.
 */
public final class ByteUtil {

    /**
     * A {@link Converter} that converts between {@link ByteData}s and hexadecimal {@link String}s.
     */
    public static final Converter<ByteData, String> STRING_CONVERTER = Converter.from(ByteData::toHex, ByteData::fromHex);

    private ByteUtil() {
    }

    /**
     * Determine the smaller of two byte strings when compared lexicographically using unsigned values.
     *
     * @param b1 first byte string
     * @param b2 second byte string
     * @return {@code b1} if {@code b1 <= b2}, otherwise {@code b2}
     * @throws NullPointerException if {@code b1} or {@code b2} is null
     */
    public static ByteData min(ByteData b1, ByteData b2) {
        return b1.compareTo(b2) <= 0 ? b1 : b2;
    }

    /**
     * Determine the larger of two byte strings when compared lexicographically using unsigned values.
     *
     * @param b1 first byte string
     * @param b2 second byte string
     * @return {@code b1} if {@code b1 >= b2}, otherwise {@code b2}
     * @throws NullPointerException if {@code b1} or {@code b2} is null
     */
    public static ByteData max(ByteData b1, ByteData b2) {
        return b1.compareTo(b2) >= 0 ? b1 : b2;
    }

    /**
     * Get the next key greater than the given key in unsigned lexicographic ordering.
     *
     * <p>
     * This creates a new key simply by appending a {@code 0x00} byte to the data
     * contained in the given key.
     *
     * @param key previous key
     * @return next key after {@code key}
     * @throws NullPointerException if {@code key} is null
     */
    public static ByteData getNextKey(ByteData key) {
        return key.concat(ByteData.zeros(1));
    }

    /**
     * Determine whether {@code key2} is the next key after {@code key1}.
     *
     * @param key1 first key
     * @param key2 second key
     * @return true if {@code key2} immediately follows {@code key1}
     * @throws NullPointerException if either parameter is null
     */
    public static boolean isConsecutive(ByteData key1, ByteData key2) {
        final int key1Len = key1.size();
        final int key2Len = key2.size();
        if (key2Len != key1Len + 1)
            return false;
        if (key2.ubyteAt(key1Len) != 0x00)
            return false;
        return key2.substring(0, key1Len).equals(key1);
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
    public static ByteData getKeyAfterPrefix(ByteData prefix) {
        Preconditions.checkArgument(!prefix.isEmpty(), "empty prefix");
        for (int i = prefix.size() - 1; i >= 0; i--) {
            final int value = prefix.ubyteAt(i);
            if (value < 0xff)
                return prefix.substring(0, i).concat(ByteData.of(value + 1));
        }
        throw new IllegalArgumentException("prefix contains only 0xff bytes");
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
    public static int readInt(ByteData.Reader reader) {
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
    public static void writeInt(ByteData.Writer writer, int value) {
        writer.write(value >> 24);
        writer.write(value >> 16);
        writer.write(value >> 8);
        writer.write(value);
    }

    /**
     * Read a {@code long} as eight big-endian bytes.
     *
     * @param reader input
     * @return decoded long
     * @throws IndexOutOfBoundsException if less than eight bytes remain in {@code reader}
     * @see #writeLong writeLong()
     */
    public static long readLong(ByteData.Reader reader) {
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
    public static void writeLong(ByteData.Writer writer, long value) {
        writer.write((int)(value >> 56));
        writer.write((int)(value >> 48));
        writer.write((int)(value >> 40));
        writer.write((int)(value >> 32));
        writer.write((int)(value >> 24));
        writer.write((int)(value >> 16));
        writer.write((int)(value >> 8));
        writer.write((int)value);
    }

    /**
     * Convert byte data into a string of hex digits, or {@code "null"} for null..
     *
     * @param data byte data
     * @return string encoding of {@code data}, or {@code "null"} if {@code data} is null
     * @see ByteData#toHex
     */
    public static String toString(ByteData data) {
        return Optional.ofNullable(data)
          .map(ByteData::toHex)
          .orElse("null");
    }
}
