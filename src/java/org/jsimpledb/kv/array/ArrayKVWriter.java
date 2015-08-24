
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import com.google.common.base.Preconditions;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.jsimpledb.util.ByteUtil;

/**
 * Writes {@link ArrayKVStore} index, key, and value data, given a sorted sequence of key/value pairs.
 *
 * <p>
 * Key and value data must not exceed 2GB.
 */
public class ArrayKVWriter implements Closeable {

    private static final int BUFFER_SIZE = 1024 * 1024;

    private final BufferedOutputStream indxOutput;
    private final BufferedOutputStream keysOutput;
    private final BufferedOutputStream valsOutput;

    private int keysLength;
    private int valsLength;

    private int nextIndex;
    private byte[] prevKey;
    private byte[] baseKey;
    private int baseKeyOffset;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param indxOutput index file output
     * @param keysOutput key data file output
     * @param valsOutput value data file output
     */
    public ArrayKVWriter(OutputStream indxOutput, OutputStream keysOutput, OutputStream valsOutput) throws IOException {
        Preconditions.checkArgument(indxOutput != null, "null indxOutput");
        Preconditions.checkArgument(keysOutput != null, "null keysOutput");
        Preconditions.checkArgument(valsOutput != null, "null valsOutput");
        this.indxOutput = new BufferedOutputStream(indxOutput, BUFFER_SIZE);
        this.keysOutput = new BufferedOutputStream(keysOutput, BUFFER_SIZE);
        this.valsOutput = new BufferedOutputStream(valsOutput, BUFFER_SIZE);
    }

    /**
     * Get the number of bytes written so far to the index file.
     */
    public int getIndxLength() {
        return this.nextIndex * 8;
    }

    /**
     * Get the number of bytes written so far to the key data file.
     */
    public int getKeysLength() {
        return this.keysLength;
    }

    /**
     * Get the number of bytes written so far to the value data file.
     */
    public int getValsLength() {
        return this.valsLength;
    }

    /**
     * Write out the next key/value pair.
     *
     * @param key key
     * @param val value
     * @throws IllegalArgumentException if {@code key} is out of order (i.e., not strictly greater then the previous key)
     * @throws IllegalArgumentException if {@code key} or {@code val} is null
     * @throws IllegalStateException if either the key or data file would grow larger than 2<sup>31</sup>-1 bytes
     */
    public void writeKV(byte[] key, byte[] val) throws IOException {

        // Sanity checks
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(val != null, "null value");
        Preconditions.checkArgument(this.prevKey == null || ByteUtil.compare(key, this.prevKey) > 0, "key <= previous key");
        Preconditions.checkState((this.nextIndex * 8) + 8 > 0, "too much index data");
        Preconditions.checkState(this.keysLength == 0 || this.keysLength + key.length > 0, "too much key data");
        Preconditions.checkState(this.valsLength == 0 || this.valsLength + val.length > 0, "too much value data");

        // Write key index entry and data
        if ((this.nextIndex & 0x1f) == 0) {

            // Write a base key index entry every 32 entries
            this.writeIndxValue(this.keysLength);
            this.baseKeyOffset = this.keysLength;
            this.baseKey = key;

            // Write key data
            this.keysOutput.write(key);
            this.keysLength += key.length;
        } else {

            // Match maximal prefix of most recent base key, up to 255 bytes
            final int maxPrefixLength = Math.min(Math.min(this.baseKey.length, key.length), 0xff);
            int prefixLength = 0;
            while (prefixLength < maxPrefixLength && key[prefixLength] == this.baseKey[prefixLength])
                prefixLength++;
            final int suffixLength = key.length - prefixLength;
            assert suffixLength > 0;

            // Write encoded { base key prefix length, offset to key suffix }
            final int suffixRelativeOffset = this.keysLength - this.baseKeyOffset;
            Preconditions.checkState((suffixRelativeOffset & 0xff000000) == 0, "key(s) too long");
            this.writeIndxValue(prefixLength << 24 | suffixRelativeOffset);

            // Write key data - suffix only
            this.keysOutput.write(key, prefixLength, suffixLength);
            this.keysLength += suffixLength;
        }

        // Write value index entry
        this.writeIndxValue(this.valsLength);

        // Write value data
        this.valsOutput.write(val);
        this.valsLength += val.length;

        // Update state
        this.prevKey = key;
        this.nextIndex++;
    }

    private void writeIndxValue(int offset) throws IOException {
        this.indxOutput.write(offset >> 24);
        this.indxOutput.write(offset >> 16);
        this.indxOutput.write(offset >> 8);
        this.indxOutput.write(offset);
    }

    /**
     * Flush all three outputs.
     */
    public void flush() throws IOException {
        this.indxOutput.flush();
        this.keysOutput.flush();
        this.valsOutput.flush();
    }

    /**
     * Close all three outputs.
     */
    @Override
    public void close() throws IOException {
        if (this.closed)
            return;
        this.closed = true;
        this.indxOutput.close();
        this.keysOutput.close();
        this.valsOutput.close();
    }
}

