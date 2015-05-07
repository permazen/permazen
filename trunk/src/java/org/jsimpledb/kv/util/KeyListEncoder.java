
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Serializes a sequence of {@code byte[]} arrays, compressing common prefixes.
 *
 * <p>
 * Keys are encoded in one of two forms:
 *  <ul>
 *  <li>{@code total-length bytes...}
 *  <li>{@code -prefix-length suffix-length suffix-bytes ...}
 *  </ul>
 * The first length ({@code total-length} or negative {@code prefix-length}) is encoded using {@link LongEncoder}.
 * The {@code suffix-length}, if present, is encoded using {@link UnsignedIntEncoder}.
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
            LongEncoder.write(out, -prefixLength);
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
            return LongEncoder.encodeLength(-prefixLength) + UnsignedIntEncoder.encodeLength(suffixLength) + suffixLength;
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
        int keyLength = KeyListEncoder.readSignedInt(input);
        final byte[] key;
        if (keyLength < 0) {
            if (prev == null)
                throw new IllegalArgumentException("null `prev' given but next key has " + -keyLength + " byte shared prefix");
            final int prefixLength = -keyLength;
            final int suffixLength = UnsignedIntEncoder.read(input);
            keyLength = prefixLength + suffixLength;
            if (keyLength < 0)
                throw new IllegalArgumentException("invalid prefix length " + prefixLength + " plus suffix length " + suffixLength);
            key = new byte[keyLength];
            System.arraycopy(prev, 0, key, 0, prefixLength);
            final int num = input.read(key, prefixLength, suffixLength);
            if (num < suffixLength)
                throw new EOFException();
        } else {
            key = new byte[keyLength];
            final int num = input.read(key);
            if (num < key.length)
                throw new EOFException();
        }
        return key;
    }

    private static int readSignedInt(InputStream input) throws IOException {
        final long longValue = LongEncoder.read(input);
        final int intValue = (int)longValue;
        Preconditions.checkArgument(intValue == (int)longValue, "read out-of-range encoded int value %s", longValue);
        return intValue;
    }
}

