
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Encodes unsigned (i.e., non-negative) {@code int} values to/from self-delimited binary, preserving sort order, and such
 * that the length of the encoding is optimized for values near zero and encoded values never begin with {@code 0xff}.
 *
 * <p>
 * The encoding uses a simple prefixing format:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Encoding Format</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Encoded Bytes</th>
 *  <th style="font-weight: bold; text-align: left">Value</th>
 * </tr>
 * <tr>
 *  <td>{@code 0x00 ... 0xfa}</td>
 *  <td>Same</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfb 0xWW}</td>
 *  <td>{@code 0xWW + 0xfb}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfc 0xWW 0xXX}</td>
 *  <td>{@code 0xWWXX + 0xfb}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfd 0xWW 0xXX 0xYY}</td>
 *  <td>{@code 0xWWXXYY + 0xfb}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfe 0xWW 0xXX 0xYY 0xZZ}</td>
 *  <td>{@code 0xWWXXYYZZ + 0xfb}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xff}</td>
 *  <td>Illegal</td>
 * </tr>
 * </table>
 * </div>
 */
public final class UnsignedIntEncoder {

    /**
     * Maximum possible length of an encoded value.
     */
    public static final int MAX_ENCODED_LENGTH = 5;

    /**
     * Minimum value that triggers a multi-byte encoding.
     */
    public static final int MIN_MULTI_BYTE_VALUE = 0xfb;                       // values 0xfb ... 0xfe prefix multi-byte values

    private UnsignedIntEncoder() {
    }

// ENCODING

    /**
     * Encode the given value.
     *
     * @param value value to encode
     * @return encoded value
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static ByteData encode(int value) {
        Preconditions.checkArgument(value >= 0, "negative value");
        if (value < MIN_MULTI_BYTE_VALUE)
            return ByteData.of((byte)value);
        value -= MIN_MULTI_BYTE_VALUE;
        int len = 1;
        int mask = 0xff000000;
        boolean encoding = false;
        final byte[] buf = new byte[MAX_ENCODED_LENGTH];
        for (int shift = 24; shift != 0; shift -= 8, mask >>= 8) {
            if (encoding || (value & mask) != 0L) {
                buf[len++] = (byte)(value >> shift);
                encoding = true;
            }
        }
        buf[len++] = (byte)value;
        buf[0] = (byte)(MIN_MULTI_BYTE_VALUE + len - 2);
        return ByteData.of(buf, 0, len);
    }

    /**
     * Encode the given value.
     *
     * @param writer destination for the encoded value
     * @param value value to encode
     * @throws IllegalArgumentException if {@code value} is negative
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public static void write(ByteData.Writer writer, int value) {
        Preconditions.checkArgument(writer != null, "null writer");
        writer.write(UnsignedIntEncoder.encode(value));
    }

    /**
     * Encode the given value.
     *
     * @param out destination for the encoded value
     * @param value value to encode
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code value} is negative
     * @throws IllegalArgumentException if {@code out} is null
     */
    public static void write(OutputStream out, int value) throws IOException {
        Preconditions.checkArgument(out != null, "null out");
        UnsignedIntEncoder.encode(value).writeTo(out);
    }

    /**
     * Encode the given value.
     *
     * @param buf destination for the encoded value
     * @param value value to encode
     * @throws IllegalArgumentException if {@code value} is negative
     * @throws java.nio.BufferOverflowException if {@code buf} overflows
     * @throws java.nio.ReadOnlyBufferException if {@code buf} is read-only
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public static void write(ByteBuffer buf, int value) {
        Preconditions.checkArgument(buf != null, "null buf");
        UnsignedIntEncoder.encode(value).writeTo(buf);
    }

    /**
     * Determine the length (in bytes) of the encoded value.
     *
     * @param value value to encode
     * @return the length of the encoded value, a value between one and {@link #MAX_ENCODED_LENGTH}
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static int encodeLength(int value) {
        Preconditions.checkArgument(value >= 0, "negative value");
        value -= MIN_MULTI_BYTE_VALUE;
        if (value < 0)
            return 1;
        int length = 2;
        while ((value >>= 8) != 0)
            length++;
        return length;
    }

// DECODING

    /**
     * Decode the given value.
     *
     * @param data encoded value
     * @return decoded value
     * @throws IllegalArgumentException if {@code data} contains an invalid encoding, or extra trailing garbage
     * @throws IllegalArgumentException if {@code data} is null
     */
    public static int decode(ByteData data) {
        Preconditions.checkArgument(data != null, "null data");
        final ByteData.Reader reader = data.newReader();
        final int value = UnsignedIntEncoder.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("value contains trailing garbage");
        return value;
    }

    /**
     * Read and decode a value from the input.
     *
     * @param reader input holding an encoded value
     * @return the decoded value, always non-negative
     * @throws IllegalArgumentException if the encoded value is truncated
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public static int read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null, "null reader");
        try {
            final int first = reader.readByte();
            int value;
            switch (first) {
            case 0xfb:
                value = reader.readByte();
                break;
            case 0xfc:
                value = (reader.readByte() << 8) | reader.readByte();
                break;
            case 0xfd:
                value = (reader.readByte() << 16) | (reader.readByte() << 8) | reader.readByte();
                break;
            case 0xfe:
                value = (reader.readByte() << 24) | (reader.readByte() << 16) | (reader.readByte() << 8) | reader.readByte();
                if (value + MIN_MULTI_BYTE_VALUE < 0)
                    throw new IllegalArgumentException("invalid unsigned int encoding with high bit set");
                break;
            case 0xff:
                throw new IllegalArgumentException("invalid unsigned int encoding starting with 0xff");
            default:
                return first;
            }
            return value + MIN_MULTI_BYTE_VALUE;
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
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static int read(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");
        final int first = input.read();
        if (first == -1)
            throw new EOFException();
        final byte[] array = new byte[UnsignedIntEncoder.decodeLength(first)];
        array[0] = (byte)first;
        for (int r, off = 1; off < array.length; off += r) {
            if ((r = input.read(array, off, array.length - off)) == -1)
                throw new EOFException();
        }
        return UnsignedIntEncoder.read(new ByteData.Reader(array));
    }

    /**
     * Read and decode a value from the given {@link ByteBuffer}.
     *
     * @param buf input source for the encoded value
     * @return the decoded value
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public static int read(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        final byte first = buf.get();
        final byte[] array = new byte[UnsignedIntEncoder.decodeLength(first)];
        array[0] = first;
        if (array.length > 1)
            buf.get(array, 1, array.length - 1);
        return UnsignedIntEncoder.read(new ByteData.Reader(array));
    }

    /**
     * Skip a value from the input.
     *
     * @param reader input holding an encoded value
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public static void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null, "null reader");
        final int first = reader.readByte();
        if (first >= MIN_MULTI_BYTE_VALUE)
            reader.skip(first - MIN_MULTI_BYTE_VALUE + 1);
    }

    /**
     * Determine the length (in bytes) of an encoded value based on the first byte.
     *
     * @param first first byte of encoded value (in lower eight bits; other bits are ignored)
     * @return the length of the encoded value (including {@code first})
     * @throws IllegalArgumentException if the lower eight bits of {@code first} equal {@code 0xff}
     */
    public static int decodeLength(int first) {
        first &= 0xff;
        Preconditions.checkArgument(first != 0xff, "invalid unsigned int encoding starting with 0xff");
        return first < MIN_MULTI_BYTE_VALUE ? 1 : first - MIN_MULTI_BYTE_VALUE + 2;
    }

// TEST METHOD

    /**
     * Test routine.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        for (String arg : args) {
            ByteData bytes = ByteData.fromHex(arg);
            System.out.println("Decoding bytes: " + bytes);
            try {
                final int value = UnsignedIntEncoder.decode(bytes);
                System.out.println(String.format("%s decodes to %d (0x%08x)", bytes, value, value));
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                System.out.println(String.format("Error decoding %s: %s", bytes, e));
            }
            int value;
            try {
                value = Integer.parseInt(arg);
            } catch (IllegalArgumentException e) {
                continue;
            }
            System.out.println(String.format("Encoding value %d (0x%08x)", value, value));
            try {
                bytes = UnsignedIntEncoder.encode(value);
                System.out.println(String.format("%d (0x%8x) encodes to %s", value, value, bytes));
            } catch (IllegalArgumentException e) {
                System.out.println(String.format("Error encoding %d (0x%8x): %s", value, value, e));
            }
        }
    }
}
