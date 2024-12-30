
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Arrays;
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
    public static final Comparator<byte[]> COMPARATOR = ByteUtil::compare;

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
        final int sharedLength = Math.min(b1.length, b2.length);
        if (b1 == b2)
            return 0;
        for (int i = 0; i < sharedLength; i++) {
            final int diff = (b1[i] & 0xff) - (b2[i] & 0xff);
            if (diff != 0)
                return Integer.signum(diff);
        }
        return Integer.signum(b1.length - b2.length);
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
        return Arrays.equals(prefix, 0, prefix.length, value, 0, prefix.length);
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
        return Arrays.equals(key1, 0, key1.length, key2, 0, key1.length);
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
        for (byte value : buf) {
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
                throw new IllegalArgumentException(String.format("invalid hex digit \"%c\"", text.charAt(pos - 1)));
            final int nib2 = Character.digit(text.charAt(pos++), 16);
            if (nib2 == -1)
                throw new IllegalArgumentException(String.format("invalid hex digit \"%c\"", text.charAt(pos - 1)));
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

    /**
     * Map a {@code byte[]} array into the range {@code [0.0, 1.0)} in a way
     * that preserves order.
     *
     * <p>
     * This allows calculations that require a notion of "distance"
     * between two keys.
     *
     * <p>
     * This method simply maps the first 6.5 bytes of {@code key} into the 52 mantissa bits
     * of a {@code double} value. Obviously, the mapping is not reversible: some keys will
     * map equal {@code double} values, but otherwise the mapping is order-preserving.
     *
     * @param key input key
     * @return nearest corresponding value between zero (inclusive) and one (exclusive)
     * @throws IllegalArgumentException if {@code key} is null
     */
    public static double toDouble(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        long bits = 0x3ff0000000000000L;
        final int length6 = Math.min(key.length, 6);
        for (int i = 0; i < length6; i++)
            bits |= (long)(key[i] & 0xff) << (44 - i * 8);
        if (key.length > 6)
            bits |= (long)(key[6] & 0xff) >> 4;
        return Double.longBitsToDouble(bits) - 1.0;
    }

    /**
     * Performs the inverse of {@link #toDouble toDouble()}.
     *
     * <p>
     * The mapping of {@link #toDouble toDouble()} is not reversible: some keys will map to the same
     * {@code double} value, so this method does not always return the original {@code byte[]} key.
     *
     * @param value input value; must be &gt;= 0.0 and &lt; 1.0
     * @return a {@code byte[]} key that maps to {@code value}
     * @throws IllegalArgumentException if {@code value} is not a number
     *  between zero (inclusive) and one (exclusive)
     */
    public static byte[] fromDouble(double value) {
        Preconditions.checkArgument(Double.isFinite(value) && value >= 0.0 && value < 1.0, "invalid value");

        // Extract bytes
        final long bits = Double.doubleToLongBits(value + 1.0);
        final byte[] bytes = new byte[] {
            (byte)(bits >> 44),
            (byte)(bits >> 36),
            (byte)(bits >> 28),
            (byte)(bits >> 20),
            (byte)(bits >> 12),
            (byte)(bits >>  4),
            (byte)(bits <<  4)
        };

        // Trim trailing zero bytes
        int length = bytes.length;
        while (length > 0 && bytes[length - 1] == 0)
            length--;
        switch (length) {
        case 0:
            return EMPTY;
        case 7:
            return bytes;
        default:
            return Arrays.copyOfRange(bytes, 0, length);
        }
    }
}
