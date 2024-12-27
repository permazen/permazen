
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

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
 * A read-only {@link KVStore} implementation is possible by implementing only
 * {@link #getRange(ByteData, ByteData, boolean) getRange()}, and a read-write implementation by also
 * implementing {@link #put put()}. However, subclasses typically provide more efficient implementations
 * of the methods listed above.
 *
 * @see KVPairIterator
 */
public abstract class AbstractKVStore implements KVStore {

    protected AbstractKVStore() {
    }

    @Override
    public ByteData get(ByteData key) {
        final KVPair pair = this.getAtLeast(key, ByteUtil.getNextKey(key));
        if (pair == null)
            return null;
        assert pair.getKey().equals(key);
        return pair.getValue();
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        if (minKey != null && maxKey != null && minKey.compareTo(maxKey) >= 0)
            return null;
        try (CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, false)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        if (minKey != null && maxKey != null && minKey.compareTo(maxKey) >= 0)
            return null;
        try (CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, true)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public void put(ByteData key, ByteData value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(ByteData key) {
        this.removeRange(key, ByteUtil.getNextKey(key));
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        try (CloseableIterator<KVPair> i = this.getRange(minKey, maxKey, false)) {
            while (i.hasNext()) {
                i.next();
                i.remove();
            }
        }
    }

    @Override
    public ByteData encodeCounter(long value) {
        final ByteData.Writer writer = ByteData.newWriter(8);
        ByteUtil.writeLong(writer, value);
        return writer.toByteData();
    }

    @Override
    public long decodeCounter(ByteData value) {
        Preconditions.checkArgument(value.size() == 8, "invalid encoded counter value size != 8");
        return ByteUtil.readLong(value.newReader());
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        if (key == null)
            throw new NullPointerException("null key");
        final ByteData previous = this.get(key);
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
