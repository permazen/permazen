
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.util.ByteUtil;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Writes {@link ArrayKVStore} index, key, and value data, given a sorted sequence of key/value pairs.
 *
 * <p>
 * Key and value data must not exceed 2GB (each separately).
 */
public class ArrayKVWriter implements Closeable {

    private static final int BUFFER_SIZE = 1024 * 1024;

    // Flags used by writeMerged()
    private static final int MERGE_KV = 0x01;
    private static final int MERGE_PUT = 0x02;
    private static final int MERGE_ADJUST = 0x04;

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
    public ArrayKVWriter(OutputStream indxOutput, OutputStream keysOutput, OutputStream valsOutput) {
        Preconditions.checkArgument(indxOutput != null, "null indxOutput");
        Preconditions.checkArgument(keysOutput != null, "null keysOutput");
        Preconditions.checkArgument(valsOutput != null, "null valsOutput");
        this.indxOutput = new BufferedOutputStream(indxOutput, BUFFER_SIZE);
        this.keysOutput = new BufferedOutputStream(keysOutput, BUFFER_SIZE);
        this.valsOutput = new BufferedOutputStream(valsOutput, BUFFER_SIZE);
    }

    /**
     * Get the number of bytes written so far to the index file.
     *
     * @return length of the index file
     */
    public int getIndxLength() {
        return this.nextIndex * 8;
    }

    /**
     * Get the number of bytes written so far to the key data file.
     *
     * @return length of the key data file
     */
    public int getKeysLength() {
        return this.keysLength;
    }

    /**
     * Get the number of bytes written so far to the value data file.
     *
     * @return length of the value data file
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
     * @throws IOException if an I/O error occurrs
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
            this.baseKey = this.cloneOrCopy(this.baseKey, key);

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
        this.prevKey = this.cloneOrCopy(this.prevKey, key);
        this.nextIndex++;
    }

    /**
     * Merge the given key/value pair iteration with the specified mutations and write out the merged combination.
     * What's written is the result of the key/value iteration with the mutations applied.
     *
     * <p>
     * The {@link KVStore} parameter will only be used to invoke {@link KVStore#encodeCounter KVStore.encodeCounter()}
     * and {@link KVStore#decodeCounter KVStore.decodeCounter()} in order to handle counter adjustments.
     *
     * @param kvstore callback for encoding and decoding counter values
     * @param kvIterator key/value pairs - must be sorted in order
     * @param mutations mutations to apply
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code kvs} iterates out of order
     * @throws IllegalStateException if either the key or data file would grow larger than 2<sup>31</sup>-1 bytes
     * @throws IOException if an I/O error occurrs
     */
    @SuppressWarnings("fallthrough")
    public void writeMerged(KVStore kvstore, Iterator<KVPair> kvIterator, Mutations mutations) throws IOException {

        // Sanity checks
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(kvIterator != null, "null kvIterator");
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Initialize iterators
        final Iterator<? extends KeyRange> removeIterator = mutations.getRemoveRanges().iterator();
        final Iterator<? extends Map.Entry<byte[], byte[]>> putIterator = mutations.getPutPairs().iterator();
        final Iterator<? extends Map.Entry<byte[], Long>> adjustIterator = mutations.getAdjustPairs().iterator();

        // Merge iterators and write the merged result to the ArrayKVWriter
        KVPair kv = kvIterator.hasNext() ? kvIterator.next() : null;
        KeyRange remove = removeIterator.hasNext() ? removeIterator.next() : null;
        Map.Entry<byte[], byte[]> put = putIterator.hasNext() ? putIterator.next() : null;
        Map.Entry<byte[], Long> adjust = adjustIterator.hasNext() ? adjustIterator.next() : null;
        while (true) {

            // Find the minimum key among remaining (a) key/value pairs, (b) puts, and (c) adjusts
            byte[] key = null;
            if (kv != null)
                key = kv.getKey();
            if (put != null && (key == null || ByteUtil.compare(put.getKey(), key) < 0))
                key = put.getKey();
            if (adjust != null && (key == null || ByteUtil.compare(adjust.getKey(), key) < 0))
                key = adjust.getKey();
            if (key == null)                                                        // we're done
                break;

            // Determine which inputs apply to the current key
            int inputs = 0;
            if (kv != null && Arrays.equals(kv.getKey(), key))
                inputs |= MERGE_KV;
            if (put != null && Arrays.equals(put.getKey(), key))
                inputs |= MERGE_PUT;
            if (adjust != null && Arrays.equals(adjust.getKey(), key))
                inputs |= MERGE_ADJUST;

            // Find the removal that applies to this next key, if any
            while (remove != null) {
                final byte[] removeMax = remove.getMax();
                if (removeMax != null && ByteUtil.compare(removeMax, key) <= 0) {
                    final KeyRange next = removeIterator.hasNext() ? removeIterator.next() : null;
                    assert next == null || ByteUtil.compare(next.getMin(), removeMax) > 0;
                    remove = next;
                    continue;
                }
                break;
            }
            final boolean removed = remove != null && ByteUtil.compare(remove.getMin(), key) <= 0;

            // Merge the applicable inputs
            switch (inputs) {
            case 0:                                                     // k/v store entry was removed
                break;
            case MERGE_KV:
                if (!removed)
                    this.writeKV(kv.getKey(), kv.getValue());
                break;
            case MERGE_PUT:
            case MERGE_PUT | MERGE_KV:
                this.writeKV(put.getKey(), put.getValue());
                break;
            case MERGE_ADJUST:                                          // adjusted a non-existent value; ignore
                break;
            case MERGE_ADJUST | MERGE_KV:
                if (removed)                                            // adjusted a removed value; ignore
                    break;
                // FALLTHROUGH
            case MERGE_ADJUST | MERGE_PUT:
            case MERGE_ADJUST | MERGE_PUT | MERGE_KV:                   // adjusted a put value
            {
                final byte[] encodedCount = (inputs & MERGE_PUT) != 0 ? put.getValue() : kv.getValue();
                final long counter;
                try {
                    counter = kvstore.decodeCounter(encodedCount);
                } catch (IllegalArgumentException e) {
                    break;
                }
                final byte[] value = kvstore.encodeCounter(counter + adjust.getValue());
                this.writeKV(key, value);
                break;
            }
            default:
                throw new RuntimeException();
            }

            // Advance k/v, put, and/or adjust
            if ((inputs & MERGE_KV) != 0) {
                assert kv != null;
                final KVPair next = kvIterator.hasNext() ? kvIterator.next() : null;
                assert next == null || ByteUtil.compare(next.getKey(), kv.getKey()) > 0;
                kv = next;
            }
            if ((inputs & MERGE_PUT) != 0) {
                assert put != null;
                final Map.Entry<byte[], byte[]> next = putIterator.hasNext() ? putIterator.next() : null;
                assert next == null || ByteUtil.compare(next.getKey(), put.getKey()) > 0;
                put = next;
            }
            if ((inputs & MERGE_ADJUST) != 0) {
                assert adjust != null;
                final Map.Entry<byte[], Long> next = adjustIterator.hasNext() ? adjustIterator.next() : null;
                assert next == null || ByteUtil.compare(next.getKey(), adjust.getKey()) > 0;
                adjust = next;
            }
        }

        // Close iterators
        for (Iterator<?> i : new Iterator<?>[] { removeIterator, putIterator, adjustIterator }) {
            if (i instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)i).close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    private void writeIndxValue(int offset) throws IOException {
        this.indxOutput.write(offset >> 24);
        this.indxOutput.write(offset >> 16);
        this.indxOutput.write(offset >> 8);
        this.indxOutput.write(offset);
    }

    /**
     * Flush all three outputs.
     *
     * @throws IOException if an I/O error occurrs
     */
    public void flush() throws IOException {
        this.indxOutput.flush();
        this.keysOutput.flush();
        this.valsOutput.flush();
    }

    /**
     * Close all three outputs.
     *
     * @throws IOException if an I/O error occurrs
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

    // Copy array if we have to, otherwise just overwrite the previous copy if the array length hasn't chagned
    private byte[] cloneOrCopy(byte[] dest, byte[] src) {
        assert src != null;
        if (dest == null || dest.length != src.length)
            return src.clone();
        System.arraycopy(src, 0, dest, 0, src.length);
        return dest;
    }
}

