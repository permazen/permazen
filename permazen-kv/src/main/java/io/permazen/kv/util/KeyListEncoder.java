
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import io.permazen.kv.KVPair;
import io.permazen.util.LongEncoder;
import io.permazen.util.UnsignedIntEncoder;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Serializes a sequence of {@code byte[]} arrays, compressing consecutive common prefixes.
 *
 * <p>
 * Keys are encoded/decoded by {@link #read read()} and {@link #write write()} in one of two forms:
 *  <ul>
 *  <li>{@code total-length bytes...}
 *  <li>{@code -prefix-length suffix-length suffix-bytes ...}
 *  </ul>
 * The first length ({@code total-length} or negative {@code prefix-length}) is encoded using {@link LongEncoder}.
 * The {@code suffix-length}, if present, is encoded using {@link UnsignedIntEncoder}.
 *
 * <p>
 * Support for encoding and decoding an entire iteration of key/value pairs is supported via
 * {@link #readPairs readPairs()} and {@link #writePairs writePairs()}.
 */
public final class KeyListEncoder {

    private KeyListEncoder() {
    }

    /**
     * Write the next key, compressing its common prefix with the previous key (if any).
     *
     * @param out output stream
     * @param key key to write
     * @param prev previous key, or null for none
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code out} or {@code key} is null
     */
    public static void write(OutputStream out, byte[] key, byte[] prev) throws IOException {
        Preconditions.checkArgument(out != null, "null out");
        Preconditions.checkArgument(key != null, "null key");
        int prefixLength = 0;
        if (prev != null) {
            while (prefixLength < key.length && prefixLength < prev.length && key[prefixLength] == prev[prefixLength])
                prefixLength++;
        }
        if (prefixLength > 1) {
            final int suffixLength = key.length - prefixLength;
            LongEncoder.write(out, ~(prefixLength - 2));
            UnsignedIntEncoder.write(out, suffixLength);
            out.write(key, prefixLength, suffixLength);
        } else {
            LongEncoder.write(out, key.length);
            out.write(key);
        }
    }

    /**
     * Calculate the number of bytes that would be required to write the next key via {@link #write write()}.
     *
     * @param key key to write
     * @param prev previous key, or null for none
     * @return number of bytes to be written by {@link #write write(out, key, prev)}
     * @throws IllegalArgumentException if {@code key} is null
     */
    public static int writeLength(byte[] key, byte[] prev) {
        Preconditions.checkArgument(key != null, "null key");
        int prefixLength = 0;
        if (prev != null) {
            while (prefixLength < key.length && prefixLength < prev.length && key[prefixLength] == prev[prefixLength])
                prefixLength++;
        }
        if (prefixLength > 1) {
            final int suffixLength = key.length - prefixLength;
            return LongEncoder.encodeLength(~(prefixLength - 2)) + UnsignedIntEncoder.encodeLength(suffixLength) + suffixLength;
        } else
            return LongEncoder.encodeLength(key.length) + key.length;
    }

    /**
     * Read the next key.
     *
     * @param input input stream
     * @param prev previous key, or null for none
     * @return next key
     * @throws IOException if an I/O error occurs
     * @throws EOFException if an unexpected EOF is encountered
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if {@code input} contains invalid data
     */
    public static byte[] read(InputStream input, byte[] prev) throws IOException {
        Preconditions.checkArgument(input != null, "null input");

        // Get encoded length of prefix
        int keyLength = KeyListEncoder.readSignedInt(input);
        final byte[] key;
        int prefixLength;

        // Decode prefix length and read prefix, if any
        if (keyLength < 0) {
            if (prev == null)
                throw new IllegalArgumentException("null `prev' given but next key has " + -keyLength + " byte shared prefix");
            prefixLength = ~keyLength + 2;
            final int suffixLength = UnsignedIntEncoder.read(input);
            keyLength = prefixLength + suffixLength;
            if (keyLength < 0)
                throw new IllegalArgumentException("invalid prefix length " + prefixLength + " plus suffix length " + suffixLength);
            key = new byte[keyLength];
            System.arraycopy(prev, 0, key, 0, prefixLength);
        } else {
            key = new byte[keyLength];
            prefixLength = 0;
        }

        // Read suffix
        while (prefixLength < key.length) {
            final int num = input.read(key, prefixLength, key.length - prefixLength);
            if (num == -1)
                throw new EOFException();
            prefixLength += num;
        }

        // Done
        return key;
    }

    /**
     * Encode an iteration of key/value pairs.
     *
     * @param kvpairs key/value pair iteration
     * @param output encoded output
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void writePairs(Iterator<KVPair> kvpairs, OutputStream output) throws IOException {

        // Sanity check
        Preconditions.checkArgument(kvpairs != null, "null kvpairs");
        Preconditions.checkArgument(output != null, "null output");

        // Write pairs
        byte[] prev = null;
        while (kvpairs.hasNext()) {
            final KVPair kv = kvpairs.next();
            final byte[] key = kv.getKey();
            final byte[] value = kv.getValue();
            KeyListEncoder.write(output, key, prev);
            KeyListEncoder.write(output, value, null);
            prev = key;
        }

        // Write special terminator byte
        output.write(0xff);
    }

    /**
     * Determine the number of bytes that would be written by {@link #writePairs writePairs()}.
     *
     * @param kvpairs key/value pair iteration
     * @return encoded length of this instance
     * @throws IllegalArgumentException if {@code kvpairs} is null
     */
    public static long writePairsLength(Iterator<KVPair> kvpairs) {
        long total = 1;
        byte[] prev = null;
        while (kvpairs.hasNext()) {
            final KVPair kv = kvpairs.next();
            final byte[] key = kv.getKey();
            final byte[] value = kv.getValue();
            total += KeyListEncoder.writeLength(key, prev);
            total += KeyListEncoder.writeLength(value, null);
            prev = key;
        }
        return total;
    }

    /**
     * Decode an iteration of key/value pairs previously encoded by {@link #writePairs writePairs()}.
     *
     * <p>
     * If an {@link IOException} occurs during iteration, the returned {@link Iterator} wraps it in a {@link RuntimeException}.
     *
     * <p>
     * If invalid input is encountered during iteration, the returned {@link Iterator}
     * will throw an {@link IllegalArgumentException}.
     *
     * @param input encoded input
     * @return iteration of key/value pairs
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static Iterator<KVPair> readPairs(final InputStream input) {

        // Sanity check
        Preconditions.checkArgument(input != null, "null input");

        // Decode
        return new AbstractIterator<KVPair>() {

            private final BufferedInputStream in = new BufferedInputStream(input, 1024);
            private byte[] prev;

            @Override
            protected KVPair computeNext() {
                try {

                    // Check for terminator
                    this.in.mark(1);
                    final int b = in.read();
                    if (b == -1)
                        throw new EOFException("truncated input");
                    if (b == 0xff)
                        return endOfData();
                    this.in.reset();

                    // Read next k/v pair
                    final byte[] key = KeyListEncoder.read(this.in, this.prev);
                    final byte[] value = KeyListEncoder.read(this.in, null);
                    this.prev = key;

                    // Done
                    return new KVPair(key, value);
                } catch (IOException e) {
                    throw new RuntimeException("I/O error during iteration", e);
                }
            }
        };
    }

    private static int readSignedInt(InputStream input) throws IOException {
        final long longValue = LongEncoder.read(input);
        final int intValue = (int)longValue;
        Preconditions.checkArgument((long)intValue == longValue, "read out-of-range encoded int value %s", longValue);
        return intValue;
    }
}
