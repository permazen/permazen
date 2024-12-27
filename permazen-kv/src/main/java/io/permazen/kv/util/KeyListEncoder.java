
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;
import io.permazen.util.UnsignedIntEncoder;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Serializes a sequence of {@code byte[]} arrays, or {@code byte[]} key/value pairs, using a simple
 * key prefix compression scheme.
 *
 * <p>
 * Keys are encoded/decoded by {@link #read read()} and {@link #write write()} in one of two forms:
 *  <ul>
 *  <li>{@code total-length bytes...}
 *  <li>{@code -prefix-length suffix-length suffix-bytes ...}
 *  </ul>
 * In the first form, the {@code total-length} is encoded using {@link LongEncoder} as a positive number
 * and is followed by that many bytes consituting the key. In the second form, the length of the key prefix
 * that matches the previous key is encoded as a negative value using {@link LongEncoder}, followed by the
 * number of suffix bytes encoded via {@link UnsignedIntEncoder}, followed by that many suffix bytes.
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
    public static void write(OutputStream out, ByteData key, ByteData prev) throws IOException {
        Preconditions.checkArgument(out != null, "null out");
        Preconditions.checkArgument(key != null, "null key");
        final int keySize = key.size();
        final int prefixLength = prev != null ? ByteData.numEqual(key, 0, prev, 0) : 0;
        if (prefixLength > 1) {
            final int suffixLength = keySize - prefixLength;
            LongEncoder.write(out, ~(prefixLength - 2));
            UnsignedIntEncoder.write(out, suffixLength);
            key.substring(prefixLength).writeTo(out);
        } else {
            LongEncoder.write(out, keySize);
            key.writeTo(out);
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
    public static int writeLength(ByteData key, ByteData prev) {
        Preconditions.checkArgument(key != null, "null key");
        final int keySize = key.size();
        final int prefixLength = prev != null ? ByteData.numEqual(key, 0, prev, 0) : 0;
        if (prefixLength > 1) {
            final int suffixLength = keySize - prefixLength;
            return LongEncoder.encodeLength(~(prefixLength - 2)) + UnsignedIntEncoder.encodeLength(suffixLength) + suffixLength;
        } else
            return LongEncoder.encodeLength(keySize) + keySize;
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
    public static ByteData read(InputStream input, ByteData prev) throws IOException {
        Preconditions.checkArgument(input != null, "null input");

        // Get encoded length of prefix
        int keyLength = KeyListEncoder.readSignedInt(input);
        final int prefixLength;

        // Decode prefix length (if any)
        if (keyLength < 0) {
            if (prev == null) {
                throw new IllegalArgumentException(String.format(
                  "null \"prev\" given but next key has %d byte shared prefix", -keyLength));
            }
            prefixLength = ~keyLength + 2;
            final int suffixLength = UnsignedIntEncoder.read(input);
            if ((keyLength = prefixLength + suffixLength) < 0) {
                throw new IllegalArgumentException(String.format(
                  "invalid prefix length %d plus suffix length %d", prefixLength, suffixLength));
            }
        } else
            prefixLength = 0;

        // Allocate buffer
        final ByteData.Writer keyWriter = ByteData.newWriter(keyLength);

        // Copy prefix bytes (if any)
        if (prefixLength > 0)
            keyWriter.write(prev.substring(0, prefixLength));

        // Copy suffix bytes (if any)
        final int suffixLength = keyLength - prefixLength;
        if (suffixLength > 0) {
            final byte[] suffix = new byte[suffixLength];
            for (int r, off = 0; off < suffixLength; off += r) {
                if ((r = input.read(suffix, off, suffixLength - off)) == -1)
                    throw new EOFException();
            }
            keyWriter.write(suffix);
        }

        // Done
        return keyWriter.toByteData();
    }

    /**
     * Encode an iteration of key/value pairs.
     *
     * <p>
     * Each key/value pair is encoded by encoding the key using prefix compression from the previous key,
     * followed by the value with no prefix compression. A final {@code 0xff} byte terminates the sequence.
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
        ByteData prev = null;
        while (kvpairs.hasNext()) {
            final KVPair kv = kvpairs.next();
            final ByteData key = kv.getKey();
            final ByteData value = kv.getValue();
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
     * @throws IllegalArgumentException if the result is larger than {@link Long#MAX_VALUE}
     */
    public static long writePairsLength(Iterator<KVPair> kvpairs) {
        long total = 1;
        ByteData prev = null;
        while (kvpairs.hasNext()) {
            final KVPair kv = kvpairs.next();
            final ByteData key = kv.getKey();
            final ByteData value = kv.getValue();
            total += KeyListEncoder.writeLength(key, prev);
            total += KeyListEncoder.writeLength(value, null);
            if (total < 0)
                throw new IllegalArgumentException("total is too large");
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
     * <p>
     * Decoding stops after reading the end of stream or a terminating {@code 0xff} byte.
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
            private ByteData prev;

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
                    final ByteData key = KeyListEncoder.read(this.in, this.prev);
                    final ByteData value = KeyListEncoder.read(this.in, null);
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
