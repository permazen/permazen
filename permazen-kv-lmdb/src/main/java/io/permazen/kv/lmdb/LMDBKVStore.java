
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableTracker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link io.permazen.kv.KVStore} view of a LMDB transaction.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources.
 *
 * @param <T> buffer type
 */
public abstract class LMDBKVStore<T> extends AbstractKVStore implements CloseableKVStore {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final CloseableTracker cursorTracker = new CloseableTracker();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Dbi<T> db;
    private final Txn<T> tx;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Closing this instance does <i>not</i> close the underlying transaction.
     *
     * @param db LMDB database
     * @param tx LMDB transaction
     * @throws IllegalArgumentException if {@code db} or {@code tx} is null
     */
    protected LMDBKVStore(Dbi<T> db, Txn<T> tx) {
        Preconditions.checkArgument(db != null, "null db");
        Preconditions.checkArgument(tx != null, "null tx");
        this.db = db;
        this.tx = tx;
        if (this.log.isTraceEnabled())
            this.log.trace("created " + this);
    }

    /**
     * Get the {@link Txn} associated with this instance.
     *
     * @return associated transaction
     */
    public Txn<T> getTransaction() {
        return this.tx;
    }

    /**
     * Get the {@link Dbi} associated with this instance.
     *
     * @return associated database handle
     */
    public Dbi<T> getDatabase() {
        return this.db;
    }

    /**
     * Determine if this instance is closed.
     *
     * @return true if closed, false if still open
     */
    public boolean isClosed() {
        return this.closed.get();
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        key = this.addPrefix(key);
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        this.cursorTracker.poll();
        return this.unwrap(this.db.get(this.tx, this.wrap(key, false)), true);
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        Preconditions.checkArgument(minKey == null || maxKey == null || ByteUtil.compare(minKey, maxKey) <= 0, "minKey > maxKey");
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        this.cursorTracker.poll();
        final CursorIterable<T> cursorIterable = this.db.iterate(this.tx, this.getKeyRange(minKey, maxKey, reverse));
        final Iterator<KVPair> i = Iterators.transform(cursorIterable.iterator(),
          kv -> new KVPair(this.delPrefix(this.unwrap(kv.key(), false)), this.unwrap(kv.val(), true)));
        final CloseableIterator<KVPair> ci = CloseableIterator.wrap(i, cursorIterable);
        this.cursorTracker.add(ci, new CloseableAutoCloseable(cursorIterable));
        return ci;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        key = this.addPrefix(key);
        value.getClass();
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        this.cursorTracker.poll();
        final boolean success = this.db.put(this.tx, this.wrap(key, false), this.wrap(value, true));
        assert success : "put failed";
    }

    @Override
    public void remove(byte[] key) {
        key = this.addPrefix(key);
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        this.cursorTracker.poll();
        this.db.delete(this.tx, this.wrap(key, true));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        Preconditions.checkArgument(minKey == null || maxKey == null || ByteUtil.compare(minKey, maxKey) <= 0, "minKey > maxKey");
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        this.cursorTracker.poll();

        // Special case: remove all
        if (maxKey == null && (minKey == null || minKey.length == 0)) {
            this.db.drop(this.tx);
            return;
        }

        // Remove them one-at-a-time
        try (CursorIterable<T> iterable = this.db.iterate(this.tx, this.getKeyRange(minKey, maxKey, false))) {
            final Iterator<CursorIterable.KeyVal<T>> i = iterable.iterator();
            while (i.hasNext()) {
                i.next();
                i.remove();
            }
        }
    }

// Utility

    /**
     * Get the {@link KeyRange} corresponding to the given parameters.
     *
     * @param minKey minimum key (inclusive), or null for none
     * @param maxKey maximum key (exclusive), or null for none
     * @param reverse true for reverse ordering, false for forward ordering
     * @return {@link KeyRange} instance
     */
    public KeyRange<T> getKeyRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        final T min = this.wrap(this.addPrefix(minKey != null && minKey.length > 0 ? minKey : ByteUtil.EMPTY), false);
        final T max = maxKey != null ? this.wrap(this.addPrefix(maxKey), false) : null;
        if (reverse) {
            if (max == null)
                return min != null ? KeyRange.atMostBackward(min) : KeyRange.allBackward();
            else
                return min != null ? KeyRange.openClosedBackward(max, min) : KeyRange.greaterThanBackward(max);
        } else {
            if (max == null)
                return min != null ? KeyRange.atLeast(min) : KeyRange.all();
            else
                return min != null ? KeyRange.closedOpen(min, max) : KeyRange.lessThan(max);
        }
    }

    /**
     * Wrap the given {@link byte[]} array in a buffer appropriate for this instance.
     *
     * @param buf byte array data, or possibly null
     * @param copy if true, then changes to the data in either {@code buf} or the returned buffer must not affect the other
     * @return null if {@code buf} is null, otherwise a buffer containing the data in {@code buf}
     */
    protected abstract T wrap(byte[] buf, boolean copy);

    /**
     * Unwrap the given buffer, returning its contents as a {@link byte[]} array.
     *
     * @param buf a buffer containing {@code byte[]} array data, or possibly null
     * @param copy if true, then changes to the data in either {@code buf} or the returned array must not affect the other
     * @return byte array data in {@code buf}, or null if {@code buf} is null
     */
    protected abstract byte[] unwrap(T buf, boolean copy);

    private byte[] addPrefix(byte[] data) {
        if (data == null)
            return data;
        final byte[] data2 = new byte[data.length + 1];
        System.arraycopy(data, 0, data2, 1, data.length);
        return data2;
    }

    private byte[] delPrefix(byte[] data) {
        if (data == null)
            return data;
        if (data.length == 0)
            throw new RuntimeException("internal error: zero length key");
        if (data[0] != 0)
            throw new RuntimeException("internal error: non-zero first byte");
        final byte[] data2 = new byte[data.length - 1];
        System.arraycopy(data, 1, data2, 0, data2.length);
        return data2;
    }

// Closeable

    /**
     * Close this instance.
     *
     * <p>
     * This closes any unclosed iterators; it does <i>not</i> close the underlying transaction.
     */
    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true))
            this.cursorTracker.close();
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #close} to close any unclosed iterators.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            if (!this.closed.get()) {
                this.log.warn(this + " leaked without invoking close()");
                this.close();
            }
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[db=" + this.db
          + ",tx=" + this.tx
          + "]";
    }

// CloseableAutoCloseable

    private static class CloseableAutoCloseable implements Closeable {

        private final AutoCloseable item;
        private final AtomicBoolean closed = new AtomicBoolean();

        CloseableAutoCloseable(AutoCloseable item) {
            this.item = item;
        }

        @Override
        public void close() throws IOException {
            if (this.closed.compareAndSet(false, true)) {
                try {
                    this.item.close();
                } catch (Error | RuntimeException | IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
