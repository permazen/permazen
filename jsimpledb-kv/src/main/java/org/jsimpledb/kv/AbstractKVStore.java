
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

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
 *      iterating through the range of keys and removing them one-by-one via {@link Iterator#remove}.</li>
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
        return pair != null && Arrays.equals(pair.getKey(), key) ? pair.getValue() : null;
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;
        final Iterator<KVPair> i = this.getRange(minKey, maxKey, false);
        try {
            return i.hasNext() ? i.next() : null;
        } finally {
            this.closeIfPossible(i);
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;
        final Iterator<KVPair> i = this.getRange(minKey, maxKey, true);
        try {
            return i.hasNext() ? i.next() : null;
        } finally {
            this.closeIfPossible(i);
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
        final Iterator<KVPair> i = this.getRange(minKey, maxKey, false);
        try {
            while (i.hasNext()) {
                i.next();
                i.remove();
            }
        } finally {
            this.closeIfPossible(i);
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
        this.put(key, this.encodeCounter(this.decodeCounter(previous) + amount));
    }

    private void closeIfPossible(Iterator<KVPair> i) {
        if (i instanceof AutoCloseable) {
            try {
                ((AutoCloseable)i).close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}

