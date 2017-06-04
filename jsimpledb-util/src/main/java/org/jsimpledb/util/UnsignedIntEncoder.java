
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

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
 * <table border="1" cellpadding="3" cellspacing="0" summary="Encoding Format">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Encoded Bytes</th>
 *  <th align="left">Value</th>
 * </tr>
 * <tr>
 *  <td>{@code 0x00 ... 0xfa}</td>
 *  <td>Same</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfb 0xWW}</td>
 *  <td>{@code 0xWW}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfc 0xWW 0xXX}</td>
 *  <td>{@code 0xWWXX}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfd 0xWW 0xXX 0xYY}</td>
 *  <td>{@code 0xWWXXYY}</td>
 * </tr>
 * <tr>
 *  <td>{@code 0xfe 0xWW 0xXX 0xYY 0xZZ}</td>
 *  <td>{@code 0xWWXXYYZZ}</td>
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

    /**
     * Encode the given value.
     *
     * @param value value to encode
     * @return encoded value
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static byte[] encode(int value) {
        final ByteWriter writer = new ByteWriter(UnsignedIntEncoder.encodeLength(value));
        UnsignedIntEncoder.write(writer, value);
        return writer.getBytes();
    }

    /**
     * Decode the given value.
     *
     * @param data encoded value
     * @return decoded value
     * @throws IllegalArgumentException if {@code bytes} contains an invalid encoding, or extra trailing garbage
     */
    public static int decode(byte[] data) {
        final ByteReader reader = new ByteReader(data);
        final int value = UnsignedIntEncoder.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("encoded value contains extra trailing garbage");
        return value;
    }

    /**
     * Encode the given value to the output.
     *
     * @param writer destination for the encoded value
     * @param value value to encode
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static void write(ByteWriter writer, int value) {
        writer.makeRoom(MAX_ENCODED_LENGTH);
        writer.len += UnsignedIntEncoder.encode(value, writer.buf, writer.len);
    }

    /**
     * Encode the given value and write it to the given {@link OutputStream}.
     *
     * @param out destination for the encoded value
     * @param value value to encode
     * @throws IOException if an I/O error occurs
     * @throws NullPointerException if {@code out} is null
     */
    public static void write(OutputStream out, int value) throws IOException {
        final byte[] array = new byte[UnsignedIntEncoder.MAX_ENCODED_LENGTH];
        final int nbytes = UnsignedIntEncoder.encode(value, array, 0);
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
    public static void write(ByteBuffer buf, int value) {
        final byte[] array = new byte[UnsignedIntEncoder.MAX_ENCODED_LENGTH];
        final int nbytes = UnsignedIntEncoder.encode(value, array, 0);
        buf.put(array, 0, nbytes);
    }

    /**
     * Read and decode a value from the input.
     *
     * @param reader input holding an encoded value
     * @return the decoded value, always non-negative
     * @throws IllegalArgumentException if the encoded value is truncated
     * @throws IllegalArgumentException if an invalid encoding is encountered
     */
    public static int read(ByteReader reader) {
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
     * @throws NullPointerException if {@code input} is null
     */
    public static int read(InputStream input) throws IOException {
        final int first = input.read();
        if (first == -1)
            throw new EOFException();
        final byte[] array = new byte[UnsignedIntEncoder.decodeLength(first)];
        array[0] = (byte)first;
        for (int i = 1; i < array.length; i++) {
            final int next = input.read();
            if (next == -1)
                throw new EOFException();
            array[i] = (byte)next;
        }
        return UnsignedIntEncoder.read(new ByteReader(array));
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
    public static int read(ByteBuffer buf) {
        final byte first = buf.get();
        final byte[] array = new byte[UnsignedIntEncoder.decodeLength(first)];
        array[0] = first;
        if (array.length > 1)
            buf.get(array, 1, array.length - 1);
        return UnsignedIntEncoder.read(new ByteReader(array));
    }

    /**
     * Skip a value from the input.
     *
     * @param reader input holding an encoded value
     */
    public static void skip(ByteReader reader) {
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

    /**
     * Determine the length (in bytes) of the encoded value.
     *
     * @param value value to encode
     * @return the length of the encoded value, a value between one and {@link #MAX_ENCODED_LENGTH}
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static int encodeLength(int value) {
        Preconditions.checkArgument(value >= 0, "value < 0");
        value -= MIN_MULTI_BYTE_VALUE;
        if (value < 0)
            return 1;
        int length = 2;
        while ((value >>= 8) != 0)
            length++;
        return length;
    }

    /**
     * Encode the given value and write the encoded bytes into the given buffer.
     *
     * @param value value to encode
     * @param buf output buffer
     * @param off starting offset into output buffer
     * @return the number of encoded bytes written
     * @throws IllegalArgumentException if {@code value} is negative
     * @throws NullPointerException if {@code buf} is null
     * @throws ArrayIndexOutOfBoundsException if {@code off} is negative or the encoded value exceeds the given buffer
     */
    public static int encode(int value, byte[] buf, int off) {
        Preconditions.checkArgument(value >= 0, "value < 0");
        if (value < MIN_MULTI_BYTE_VALUE) {
            buf[off] = (byte)value;
            return 1;
        }
        value -= MIN_MULTI_BYTE_VALUE;
        int len = 1;
        int mask = 0xff000000;
        boolean encoding = false;
        for (int shift = 24; shift != 0; shift -= 8, mask >>= 8) {
            if (encoding || (value & mask) != 0L) {
                buf[off + len++] = (byte)(value >> shift);
                encoding = true;
            }
        }
        buf[off + len++] = (byte)value;
        buf[off] = (byte)(MIN_MULTI_BYTE_VALUE + len - 2);
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
                System.out.println("Decoding bytes: " + ByteUtil.toString(bytes));
                try {
                    final long value = UnsignedIntEncoder.decode(bytes);
                    System.out.println("0x" + ByteUtil.toString(bytes)
                      + " decodes to " + value + " (" + String.format("0x%016x", value) + ")");
                } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                    System.out.println("Error decoding " + ByteUtil.toString(bytes) + ": " + e);
                }
            }
            Integer value = null;
            try {
                value = Integer.parseInt(arg);
            } catch (IllegalArgumentException e) {
                // ignore
            }
            if (value != null && value >= 0) {
                System.out.println("Encoding value " + value);
                final ByteWriter writer = new ByteWriter();
                UnsignedIntEncoder.write(writer, value);
                System.out.println(value + " (" + String.format("0x%016x", value)
                  + ") encodes to " + ByteUtil.toString(writer.getBytes()));
            }
        }
    }
}

