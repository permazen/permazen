
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
import java.util.Arrays;

/**
 * Encodes {@code long} values to/from binary, preserving sort order, and such that the length of the
 * encoding is optimized for values near zero.
 *
 * <p>
 * Some examples (in numerical order):
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Encoding Examples</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th>Value (decimal)</th>
 *  <th>Value (hex)</th>
 *  <th>Length</th>
 *  <th>Bytes</th>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x8000000000000000</code></td>
 *  <td style="text-align: right"><code>-9223372036854775808</code></td>
 *  <td style="text-align: center"><code>9</code></td>
 *  <td style="text-align: right"><code>018000000000000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfeffffffffffff89</code></td>
 *  <td style="text-align: right"><code>-72057594037928055</code></td>
 *  <td style="text-align: center"><code>9</code></td>
 *  <td style="text-align: right"><code>01feffffffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfeffffffffffff8a</code></td>
 *  <td style="text-align: right"><code>-72057594037928054</code></td>
 *  <td style="text-align: center"><code>8</code></td>
 *  <td style="text-align: right"><code>0200000000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xff00000000000000</code></td>
 *  <td style="text-align: right"><code>-72057594037927936</code></td>
 *  <td style="text-align: center"><code>8</code></td>
 *  <td style="text-align: right"><code>0200000000000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffeffffffffff89</code></td>
 *  <td style="text-align: right"><code>-281474976710775</code></td>
 *  <td style="text-align: center"><code>8</code></td>
 *  <td style="text-align: right"><code>02feffffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffeffffffffff8a</code></td>
 *  <td style="text-align: right"><code>-281474976710774</code></td>
 *  <td style="text-align: center"><code>7</code></td>
 *  <td style="text-align: right"><code>03000000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffff000000000000</code></td>
 *  <td style="text-align: right"><code>-281474976710656</code></td>
 *  <td style="text-align: center"><code>7</code></td>
 *  <td style="text-align: right"><code>03000000000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffeffffffff89</code></td>
 *  <td style="text-align: right"><code>-1099511627895</code></td>
 *  <td style="text-align: center"><code>7</code></td>
 *  <td style="text-align: right"><code>03feffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffeffffffff8a</code></td>
 *  <td style="text-align: right"><code>-1099511627894</code></td>
 *  <td style="text-align: center"><code>6</code></td>
 *  <td style="text-align: right"><code>040000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffff0000000000</code></td>
 *  <td style="text-align: right"><code>-1099511627776</code></td>
 *  <td style="text-align: center"><code>6</code></td>
 *  <td style="text-align: right"><code>040000000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffeffffff89</code></td>
 *  <td style="text-align: right"><code>-4294967415</code></td>
 *  <td style="text-align: center"><code>6</code></td>
 *  <td style="text-align: right"><code>04feffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffeffffff8a</code></td>
 *  <td style="text-align: right"><code>-4294967414</code></td>
 *  <td style="text-align: center"><code>5</code></td>
 *  <td style="text-align: right"><code>0500000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffff00000000</code></td>
 *  <td style="text-align: right"><code>-4294967296</code></td>
 *  <td style="text-align: center"><code>5</code></td>
 *  <td style="text-align: right"><code>0500000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffeffff89</code></td>
 *  <td style="text-align: right"><code>-16777335</code></td>
 *  <td style="text-align: center"><code>5</code></td>
 *  <td style="text-align: right"><code>05feffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffeffff8a</code></td>
 *  <td style="text-align: right"><code>-16777334</code></td>
 *  <td style="text-align: center"><code>4</code></td>
 *  <td style="text-align: right"><code>06000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffff000000</code></td>
 *  <td style="text-align: right"><code>-16777216</code></td>
 *  <td style="text-align: center"><code>4</code></td>
 *  <td style="text-align: right"><code>06000076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffffeff89</code></td>
 *  <td style="text-align: right"><code>-65655</code></td>
 *  <td style="text-align: center"><code>4</code></td>
 *  <td style="text-align: right"><code>06feffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffffeff8a</code></td>
 *  <td style="text-align: right"><code>-65654</code></td>
 *  <td style="text-align: center"><code>3</code></td>
 *  <td style="text-align: right"><code>070000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffff0000</code></td>
 *  <td style="text-align: right"><code>-65536</code></td>
 *  <td style="text-align: center"><code>3</code></td>
 *  <td style="text-align: right"><code>070076</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffffffe89</code></td>
 *  <td style="text-align: right"><code>-375</code></td>
 *  <td style="text-align: center"><code>3</code></td>
 *  <td style="text-align: right"><code>07feff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffffffe8a</code></td>
 *  <td style="text-align: right"><code>-374</code></td>
 *  <td style="text-align: center"><code>2</code></td>
 *  <td style="text-align: right"><code>0800</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffff00</code></td>
 *  <td style="text-align: right"><code>-256</code></td>
 *  <td style="text-align: center"><code>2</code></td>
 *  <td style="text-align: right"><code>0876</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffff89</code></td>
 *  <td style="text-align: right"><code>-119</code></td>
 *  <td style="text-align: center"><code>2</code></td>
 *  <td style="text-align: right"><code>08ff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffff8a</code></td>
 *  <td style="text-align: right"><code>-118</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>09</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffff8b</code></td>
 *  <td style="text-align: right"><code>-117</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>0a</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffff8c</code></td>
 *  <td style="text-align: right"><code>-116</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>0b</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffffa9</code></td>
 *  <td style="text-align: right"><code>-87</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>28</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffffc9</code></td>
 *  <td style="text-align: right"><code>-55</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>48</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffffe9</code></td>
 *  <td style="text-align: right"><code>-23</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>68</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xfffffffffffffffe</code></td>
 *  <td style="text-align: right"><code>-2</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>7d</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0xffffffffffffffff</code></td>
 *  <td style="text-align: right"><code>-1</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>7e</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000000</code></td>
 *  <td style="text-align: right"><code>0</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>7f</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000001</code></td>
 *  <td style="text-align: right"><code>1</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>80</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000002</code></td>
 *  <td style="text-align: right"><code>2</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>81</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000071</code></td>
 *  <td style="text-align: right"><code>113</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>f0</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000077</code></td>
 *  <td style="text-align: right"><code>119</code></td>
 *  <td style="text-align: center"><code>1</code></td>
 *  <td style="text-align: right"><code>f6</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000078</code></td>
 *  <td style="text-align: right"><code>120</code></td>
 *  <td style="text-align: center"><code>2</code></td>
 *  <td style="text-align: right"><code>f700</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000177</code></td>
 *  <td style="text-align: right"><code>375</code></td>
 *  <td style="text-align: center"><code>2</code></td>
 *  <td style="text-align: right"><code>f7ff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000000178</code></td>
 *  <td style="text-align: right"><code>376</code></td>
 *  <td style="text-align: center"><code>3</code></td>
 *  <td style="text-align: right"><code>f80100</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000010077</code></td>
 *  <td style="text-align: right"><code>65655</code></td>
 *  <td style="text-align: center"><code>3</code></td>
 *  <td style="text-align: right"><code>f8ffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000000010078</code></td>
 *  <td style="text-align: right"><code>65656</code></td>
 *  <td style="text-align: center"><code>4</code></td>
 *  <td style="text-align: right"><code>f9010000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000001000077</code></td>
 *  <td style="text-align: right"><code>16777335</code></td>
 *  <td style="text-align: center"><code>4</code></td>
 *  <td style="text-align: right"><code>f9ffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000001000078</code></td>
 *  <td style="text-align: right"><code>16777336</code></td>
 *  <td style="text-align: center"><code>5</code></td>
 *  <td style="text-align: right"><code>fa01000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000100000077</code></td>
 *  <td style="text-align: right"><code>4294967415</code></td>
 *  <td style="text-align: center"><code>5</code></td>
 *  <td style="text-align: right"><code>faffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000000100000078</code></td>
 *  <td style="text-align: right"><code>4294967416</code></td>
 *  <td style="text-align: center"><code>6</code></td>
 *  <td style="text-align: right"><code>fb0100000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000010000000077</code></td>
 *  <td style="text-align: right"><code>1099511627895</code></td>
 *  <td style="text-align: center"><code>6</code></td>
 *  <td style="text-align: right"><code>fbffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0000010000000078</code></td>
 *  <td style="text-align: right"><code>1099511627896</code></td>
 *  <td style="text-align: center"><code>7</code></td>
 *  <td style="text-align: right"><code>fc010000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0001000000000077</code></td>
 *  <td style="text-align: right"><code>281474976710775</code></td>
 *  <td style="text-align: center"><code>7</code></td>
 *  <td style="text-align: right"><code>fcffffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0001000000000078</code></td>
 *  <td style="text-align: right"><code>281474976710776</code></td>
 *  <td style="text-align: center"><code>8</code></td>
 *  <td style="text-align: right"><code>fd01000000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0100000000000077</code></td>
 *  <td style="text-align: right"><code>72057594037928055</code></td>
 *  <td style="text-align: center"><code>8</code></td>
 *  <td style="text-align: right"><code>fdffffffffffffff</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x0100000000000078</code></td>
 *  <td style="text-align: right"><code>72057594037928056</code></td>
 *  <td style="text-align: center"><code>9</code></td>
 *  <td style="text-align: right"><code>fe0100000000000000</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x7fffffffffffff78</code></td>
 *  <td style="text-align: right"><code>9223372036854775672</code></td>
 *  <td style="text-align: center"><code>9</code></td>
 *  <td style="text-align: right"><code>fe7fffffffffffff00</code></td>
 * </tr>
 * <tr>
 *  <td style="text-align: right"><code>0x7fffffffffffffff</code></td>
 *  <td style="text-align: right"><code>9223372036854775807</code></td>
 *  <td style="text-align: center"><code>9</code></td>
 *  <td style="text-align: right"><code>fe7fffffffffffff87</code></td>
 * </tr>
 * </table>
 * </div>
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

// ENCODING

    /**
     * Encode the given value.
     *
     * @param value value to encode
     * @return encoded value
     */
    public static ByteData encode(long value) {
        final byte[] buf = new byte[MAX_ENCODED_LENGTH];
        int len = 1;
        if (value < MIN_SINGLE_BYTE_VALUE) {
            value += NEGATIVE_ADJUST;
            long mask = 0x00ffffffffffffffL;
            for (int shift = 56; shift != 0; shift -= 8, mask >>= 8) {
                if ((value | mask) != ~0L)
                    buf[len++] = (byte)(value >> shift);
            }
            buf[0] = (byte)(MIN_SINGLE_BYTE_ENCODED - len);
        } else if (value > MAX_SINGLE_BYTE_VALUE) {
            value += POSITIVE_ADJUST;
            long mask = 0xff00000000000000L;
            for (int shift = 56; shift != 0; shift -= 8, mask >>= 8) {
                if ((value & mask) != 0L)
                    buf[len++] = (byte)(value >> shift);
            }
            buf[0] = (byte)(MAX_SINGLE_BYTE_ENCODED + len);
        } else
            return ByteData.of((byte)(value + ZERO_ADJUST));
        buf[len++] = (byte)value;
        return ByteData.of(buf, 0, len);
    }

    /**
     * Encode the given value.
     *
     * @param writer destination for the encoded value
     * @param value value to encode
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public static void write(ByteData.Writer writer, long value) {
        Preconditions.checkArgument(writer != null, "null writer");
        writer.write(LongEncoder.encode(value));
    }

    /**
     * Encode the given value.
     *
     * @param out destination for the encoded value
     * @param value value to encode
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code out} is null
     */
    public static void write(OutputStream out, long value) throws IOException {
        Preconditions.checkArgument(out != null, "null out");
        LongEncoder.encode(value).writeTo(out);
    }

    /**
     * Encode the given value.
     *
     * @param buf destination for the encoded value
     * @param value value to encode
     * @throws java.nio.BufferOverflowException if {@code buf} overflows
     * @throws java.nio.ReadOnlyBufferException if {@code buf} is read-only
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public static void write(ByteBuffer buf, long value) {
        Preconditions.checkArgument(buf != null, "null buf");
        LongEncoder.encode(value).writeTo(buf);
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

// DECODING

    /**
     * Decode the given value.
     *
     * @param data encoded value
     * @return decoded value
     * @throws IllegalArgumentException if {@code data} contains an invalid encoding, or extra trailing garbage
     * @throws IllegalArgumentException if {@code data} is null
     */
    public static long decode(ByteData data) {
        Preconditions.checkArgument(data != null, "null data");
        final ByteData.Reader reader = data.newReader();
        final long value = LongEncoder.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("value contains trailing garbage");
        return value;
    }

    /**
     * Read and decode a value from the input.
     *
     * @param reader input holding an encoded value
     * @return the decoded value
     * @throws IllegalArgumentException if an invalid encoding is encountered
     * @throws IllegalArgumentException if the encoded value is truncated
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public static long read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null, "null reader");
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
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static long read(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");
        final int first = input.read();
        if (first == -1)
            throw new EOFException();
        final byte[] array = new byte[LongEncoder.decodeLength(first)];
        array[0] = (byte)first;
        for (int r, off = 1; off < array.length; off += r) {
            if ((r = input.read(array, off, array.length - off)) == -1)
                throw new EOFException();
        }
        return LongEncoder.read(ByteData.of(array).newReader());
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
    public static long read(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        final byte first = buf.get();
        final byte[] array = new byte[LongEncoder.decodeLength(first)];
        array[0] = first;
        if (array.length > 1)
            buf.get(array, 1, array.length - 1);
        return LongEncoder.read(ByteData.of(array).newReader());
    }

    /**
     * Skip a value from the input.
     *
     * @param reader input holding an encoded value
     * @throws IllegalArgumentException if the first byte is {@code 0xff}
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public static void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null, "null reader");
        final int remain = LongEncoder.decodeLength(reader.readByte()) - 1;
        if (remain > 0)
            reader.skip(remain);
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
            throw new IllegalArgumentException(String.format("invalid encoded value starting with 0x%02x", first));
        if (first < MIN_SINGLE_BYTE_ENCODED)
            return 1 + MIN_SINGLE_BYTE_ENCODED - first;
        if (first > MAX_SINGLE_BYTE_ENCODED)
            return 1 + first - MAX_SINGLE_BYTE_ENCODED;
        return 1;
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
                final long value = LongEncoder.decode(bytes);
                System.out.println(String.format("%s decodes to %d (0x%016x)", bytes, value, value));
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                System.out.println(String.format("Error decoding %s: %s", bytes, e));
            }
            long value;
            try {
                value = Long.parseLong(arg);
            } catch (IllegalArgumentException e) {
                continue;
            }
            System.out.println(String.format("Encoding value %d (0x%016x)", value, value));
            try {
                bytes = LongEncoder.encode(value);
                System.out.println(String.format("%d (0x%16x) encodes to %s", value, value, bytes));
            } catch (IllegalArgumentException e) {
                System.out.println(String.format("Error encoding %d (0x%16x): %s", value, value, e));
            }
        }
    }
}
