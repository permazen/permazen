
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.CloseableIterator;

import java.util.Arrays;

/**
 * Support superclass for {@link KVStore} implementations.
 *
 * <p>
 * This class provides a partial implementation via the following methods:
 * <ul>
 *  <li>A {@link #get get()} implementation based on {@link #getAtLeast getAtLeast()}</li>
 *  <li>{@link #getAtLeast getAtLeast()} and {@link #getAtMost getAtMost()} implementations based on
 *      {@link #getRange getRange()}.</li>
 *  <li>A {@link #remove remove()} implementation that delegates to {@link #removeRange removeRange()}.</li>
 *  <li>A {@link #removeRange removeRange()} implementation that delegates to {@link #getRange getRange()},
 *      iterating through the range of keys and removing them one-by-one via {@link java.util.Iterator#remove}.</li>
 *  <li>{@link #encodeCounter encodeCounter()}, {@link #decodeCounter encodeCounter()}, and
 *      {@link #adjustCounter adjustCounter()} implementations using normal reads and writes
 *      of values in big-endian encoding (does not provide any lock-free behavior).</li>
 *  <li>A {@link #put put()} implementation throwing {@link UnsupportedOperationException}</li>
 * </ul>
 *
 * <p>
 * Therefore, a read-only {@link KVStore} implementation is possible simply by implementing {@link #getRange}.
 *
 * @see KVPairIterator
 */
public abstract class AbstractKVStore implements KVStore {

    protected AbstractKVStore() {
    }

    @Override
    public byte[] get(byte[] key) {
        final KVPair pair = this.getAtLeast(key, ByteUtil.getNextKey(key));
        if (pair == null)
            return null;
        assert Arrays.equals(pair.getKey(), key);
        return pair.getValue();
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;
        try (final CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, false)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;
        try (final CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, true)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(byte[] key) {
        this.removeRange(key, ByteUtil.getNextKey(key));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        try (final CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, false)) {
            while (i.hasNext()) {
                i.next();
                i.remove();
            }
        }
    }

    @Override
    public byte[] encodeCounter(long value) {
        final ByteWriter writer = new ByteWriter(8);
        ByteUtil.writeLong(writer, value);
        return writer.getBytes();
    }

    @Override
    public long decodeCounter(byte[] value) {
        Preconditions.checkArgument(value.length == 8, "invalid encoded counter value length != 8");
        return ByteUtil.readLong(new ByteReader(value));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        if (key == null)
            throw new NullPointerException("null key");
        final byte[] previous = this.get(key);
        if (previous == null)
            return;
        final long oldValue;
        try {
            oldValue = this.decodeCounter(previous);
        } catch (IllegalArgumentException e) {
            return;                                                     // if previous value is not valid, behavior is undefined
        }
        this.put(key, this.encodeCounter(oldValue + amount));
    }
}

