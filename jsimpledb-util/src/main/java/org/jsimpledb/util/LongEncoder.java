
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Encodes {@code long} values to/from binary, preserving sort order, and such that the length of the
 * encoding is optimized for values near zero.
 *
 * <p>
 * Encoded values are guaranteed to not start with {@code 0x00} or {@code 0xff}.
 */
public final class LongEncoder {

    /**
     * Maximum possible length of an encoded value.
     */
    public static final int MAX_ENCODED_LENGTH = 9;

    /**
     * Minimum value for the first byte of a single byte encoded value.
     * Lower values indicate a multiple byte encoded negative value.
     */
    public static final int MIN_SINGLE_BYTE_ENCODED = 0x09;        // values 0x01 ... 0x08 prefix negative values

    /**
     * Maximum value for the first byte of a single byte encoded value.
     * Higher values indicate a multiple byte encoded positive value.
     */
    public static final int MAX_SINGLE_BYTE_ENCODED = 0xf6;        // values 0xf7 ... 0xfe prefix positive values

    /**
     * Adjustment applied to single byte encoded values before encoding.
     */
    public static final int ZERO_ADJUST = 127;                     // single byte value that represents zero

    /**
     * Minimum value that can be encoded as a single byte.
     */
    public static final int MIN_SINGLE_BYTE_VALUE = MIN_SINGLE_BYTE_ENCODED - ZERO_ADJUST;          // -118

    /**
     * Maximum value that can be encoded as a single byte.
     */
    public static final int MAX_SINGLE_BYTE_VALUE = MAX_SINGLE_BYTE_ENCODED - ZERO_ADJUST;          // 119

    /**
     * Adjustment applied to multi-byte encoded negative values before encoding.
     */
    public static final int NEGATIVE_ADJUST = -MIN_SINGLE_BYTE_VALUE;                               // 118

    /**
     * Adjustment applied to multi-byte encoded positive values before encoding.
     */
    public static final int POSITIVE_ADJUST = -(MAX_SINGLE_BYTE_VALUE + 1);                         // -120

    // Cutoff values at which the encoded length changes (this field is package private for testing purposes)
    static final long[] CUTOFF_VALUES = new long[] {
        0xff00000000000000L - NEGATIVE_ADJUST,      // [ 0] requires 8 bytes
        0xffff000000000000L - NEGATIVE_ADJUST,      // [ 1] requires 7 bytes
        0xffffff0000000000L - NEGATIVE_ADJUST,      // [ 2] requires 6 bytes
        0xffffffff00000000L - NEGATIVE_ADJUST,      // [ 3] requires 5 bytes
        0xffffffffff000000L - NEGATIVE_ADJUST,      // [ 4] requires 4 bytes
        0xffffffffffff0000L - NEGATIVE_ADJUST,      // [ 5] requires 3 bytes
        0xffffffffffffff00L - NEGATIVE_ADJUST,      // [ 6] requires 2 bytes
        MIN_SINGLE_BYTE_VALUE,                      // [ 7] requires 1 byte
        MAX_SINGLE_BYTE_VALUE + 1,                  // [ 8] requires 2 bytes
        0x0000000000000100L - POSITIVE_ADJUST,      // [ 9] requires 3 bytes
        0x0000000000010000L - POSITIVE_ADJUST,      // [10] requires 4 bytes
        0x0000000001000000L - POSITIVE_ADJUST,      // [11] requires 5 bytes
        0x0000000100000000L - POSITIVE_ADJUST,      // [12] requires 6 bytes
        0x0000010000000000L - POSITIVE_ADJUST,      // [13] requires 7 bytes
        0x0001000000000000L - POSITIVE_ADJUST,      // [14] requires 8 bytes
        0x0100000000000000L - POSITIVE_ADJUST,      // [15] requires 9 bytes
    };

    private LongEncoder() {
    }

    /**
     * Encode the given value.
     *
     * @param value value to encode
     * @return encoded bytes
     */
    public static byte[] encode(long value) {
        final ByteWriter writer = new ByteWriter(LongEncoder.encodeLength(value));
        LongEncoder.write(writer, value);
        return writer.getBytes();
    }

    /**
     * Decode the given value.
     *
     * @param data encoded value
     * @return decoded value
     * @throws IllegalArgumentException if {@code bytes} contains an invalid encoding, or extra trailing garbage
     */
    public static long decode(byte[] data) {
        final ByteReader reader = new ByteReader(data);
        final long value = LongEncoder.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("encoded value contains extra trailing garbage");
        return value;
    }

    /**
     * Encode the given value to the output.
     *
     * @param writer destination for the encoded value
     * @param value value to encode
     * @throws NullPointerException if {@code writer} is null
     */
    public static void write(ByteWriter writer, long value) {
        writer.makeRoom(MAX_ENCODED_LENGTH);
        writer.len += LongEncoder.encode(value, writer.buf, writer.len);
    }

    /**
     * Encode the given value and write it to the given {@link OutputStream}.
     *
     * @param out destination for the encoded value
     * @param value value to encode
     * @throws IOException if an I/O error occurs
     * @throws NullPointerException if {@code out} is null
     */
    public static void write(OutputStream out, long value) throws IOException {
        final byte[] array = new byte[LongEncoder.MAX_ENCODED_LENGTH];
        final int nbytes = LongEncoder.encode(value, array, 0);
        out.write(array, 0, nbytes);
    }

    /**
     * Encode the given value and write it to the given {@link ByteBuffer}.
     *
     * @param buf destination for the encoded value
     * @param value value to encode
     * @throws java.nio.BufferOverflowException if {@code buf} overflows
     * @throws java.nio.ReadOnlyBufferException if {@code buf} is read-only
     * @throws NullPointerException if {@code buf} is null
     */
    public static void write(ByteBuffer buf, long value) {
        final byte[] array = new byte[LongEncoder.MAX_ENCODED_LENGTH];
        final int nbytes = LongEncoder.encode(value, array, 0);
        buf.put(array, 0, nbytes);
    }

    /**
     * Read and decode a value from the input.
     *
     * @param reader input holding an encoded value
     * @return the decoded value
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws IllegalArgumentException if the encoded value is truncated
     * @throws NullPointerException if {@code reader} is null
     */
    public static long read(ByteReader reader) {
        try {
            int first = reader.readByte();
            if (first < MIN_SINGLE_BYTE_ENCODED) {
                if (first == 0x00)
                    throw new IllegalArgumentException("invalid encoded value starting with 0x00");
                long value = ~0L;
                while (first++ < MIN_SINGLE_BYTE_ENCODED)
                    value = (value << 8) | reader.readByte();
                return value - NEGATIVE_ADJUST;
            }
            if (first > MAX_SINGLE_BYTE_ENCODED) {
                if (first == 0xff)
                    throw new IllegalArgumentException("invalid encoded value starting with 0xff");
                long value = 0L;
                while (first-- > MAX_SINGLE_BYTE_ENCODED)
                    value = (value << 8) | reader.readByte();
                return value - POSITIVE_ADJUST;
            }
            return (byte)(first - ZERO_ADJUST);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("encoded value is truncated", e);
        }
    }

    /**
     * Read and decode a value from the given {@link InputStream}.
     *
     * @param input input source for the encoded value
     * @return the decoded value
     * @throws IOException if an I/O error occurs
     * @throws EOFException if an unexpected EOF is encountered
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws NullPointerException if {@code input} is null
     */
    public static long read(InputStream input) throws IOException {
        final int first = input.read();
        if (first == -1)
            throw new EOFException();
        final byte[] array = new byte[LongEncoder.decodeLength(first)];
        array[0] = (byte)first;
        for (int i = 1; i < array.length; i++) {
            final int next = input.read();
            if (next == -1)
                throw new EOFException();
            array[i] = (byte)next;
        }
        return LongEncoder.read(new ByteReader(array));
    }

    /**
     * Read and decode a value from the given {@link ByteBuffer}.
     *
     * @param buf input source for the encoded value
     * @return the decoded value
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws NullPointerException if {@code buf} is null
     */
    public static long read(ByteBuffer buf) {
        final byte first = buf.get();
        final byte[] array = new byte[LongEncoder.decodeLength(first)];
        array[0] = first;
        if (array.length > 1)
            buf.get(array, 1, array.length - 1);
        return LongEncoder.read(new ByteReader(array));
    }

    /**
     * Skip a value from the input.
     *
     * @param reader input holding an encoded value
     * @throws IllegalArgumentException if the first byte is {@code 0xff}
     */
    public static void skip(ByteReader reader) {
        final int first = reader.readByte();
        if (first == 0x00 || first == 0xff)
            throw new IllegalArgumentException("invalid encoded value starting with 0x" + Integer.toHexString(first));
        reader.skip(LongEncoder.decodeLength(first) - 1);
    }

    /**
     * Determine the length (in bytes) of an encoded value based on the first byte.
     *
     * @param first first byte of encoded value (in lower eight bits; other bits are ignored)
     * @return the length of the encoded value (including {@code first})
     * @throws IllegalArgumentException if the first byte is {@code 0xff}
     */
    public static int decodeLength(int first) {
        first &= 0xff;
        if (first == 0x00 || first == 0xff)
            throw new IllegalArgumentException("invalid encoded value starting with 0x" + Integer.toHexString(first));
        if (first < MIN_SINGLE_BYTE_ENCODED)
            return 1 + MIN_SINGLE_BYTE_ENCODED - first;
        if (first > MAX_SINGLE_BYTE_ENCODED)
            return 1 + first - MAX_SINGLE_BYTE_ENCODED;
        return 1;
    }

    /**
     * Determine the length (in bytes) of the encoded value.
     *
     * @param value value to encode
     * @return the length of the encoded value, a value between one and {@link #MAX_ENCODED_LENGTH}
     */
    public static int encodeLength(long value) {
        int index = Arrays.binarySearch(CUTOFF_VALUES, value);
        if (index < 0)
            index = ~index - 1;
        return index < 8 ? 8 - index : index - 6;
    }

    /**
     * Encode the given value and write the encoded bytes into the given buffer.
     *
     * @param value value to encode
     * @param buf output buffer
     * @param off starting offset into output buffer
     * @return the number of encoded bytes written
     * @throws ArrayIndexOutOfBoundsException if {@code off} is negative or the encoded value exceeds the given buffer
     */
    private static int encode(long value, byte[] buf, int off) {
        int len = 1;
        if (value < MIN_SINGLE_BYTE_VALUE) {
            value += NEGATIVE_ADJUST;
            long mask = 0x00ffffffffffffffL;
            for (int shift = 56; shift != 0; shift -= 8, mask >>= 8) {
                if ((value | mask) != ~0L)
                    buf[off + len++] = (byte)(value >> shift);
            }
            buf[off] = (byte)(MIN_SINGLE_BYTE_ENCODED - len);
        } else if (value > MAX_SINGLE_BYTE_VALUE) {
            value += POSITIVE_ADJUST;
            long mask = 0xff00000000000000L;
            for (int shift = 56; shift != 0; shift -= 8, mask >>= 8) {
                if ((value & mask) != 0L)
                    buf[off + len++] = (byte)(value >> shift);
            }
            buf[off] = (byte)(MAX_SINGLE_BYTE_ENCODED + len);
        } else {
            buf[off] = (byte)(value + ZERO_ADJUST);
            return 1;
        }
        buf[off + len++] = (byte)value;
        return len;
    }

    /**
     * Test routine.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        for (String arg : args) {
            byte[] bytes = null;
            try {
                bytes = ByteUtil.parse(arg);
            } catch (IllegalArgumentException e) {
                if (arg.startsWith("0x"))
                    bytes = ByteUtil.parse(arg.substring(2));
            }
            if (bytes != null) {
                final long value = LongEncoder.decode(bytes);
                System.out.println("0x" + ByteUtil.toString(bytes)
                  + " decodes to " + value + " (" + String.format("0x%016x", value) + ")");
            }
            Long value = null;
            try {
                value = Long.parseLong(arg);
            } catch (IllegalArgumentException e) {
                // ignore
            }
            if (value != null) {
                final ByteWriter writer = new ByteWriter();
                LongEncoder.write(writer, value);
                System.out.println(value + " (" + String.format("0x%016x", value)
                  + ") encodes to " + ByteUtil.toString(writer.getBytes()));
            }
        }
    }
}

