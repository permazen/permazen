
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

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
 *  <li>A {@link #getRange getRange()} implementation based on {@link KVPairIterator}.</li>
 *  <li>A {@link #remove remove()} implementation that delegates to {@link #removeRange removeRange()}.</li>
 *  <li>A {@link #removeRange removeRange()} implementation that delegates to {@link #getRange getRange()},
 *      iterating through the range of keys and removing them one-by-one via {@link Iterator#remove}.</li>
 *  <li>{@link #encodeCounter encodeCounter()}, {@link #decodeCounter encodeCounter()}, and
 *      {@link #adjustCounter adjustCounter()} implementations using normal reads and writes
 *      of values in big-endian encoding (does not provide any lock-free behavior).</li>
 * </ul>
 */
public abstract class AbstractKVStore implements KVStore {

    protected AbstractKVStore() {
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (minKey == null)
            minKey = ByteUtil.EMPTY;
        return new KVPairIterator(this, new KeyRange(minKey, maxKey), null, reverse);
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
            if (i instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)i).close();
                } catch (Exception e) {
                    // ignore
                }
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
        if (value.length != 8)
            throw new IllegalArgumentException("invalid encoded counter value: length = " + value.length + " != 8");
        return ByteUtil.readLong(new ByteReader(value));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        if (key == null)
            throw new NullPointerException("null key");
        byte[] previous = this.get(key);
        if (previous == null)
            previous = new byte[8];
        this.put(key, this.encodeCounter(this.decodeCounter(previous) + amount));
    }
}

