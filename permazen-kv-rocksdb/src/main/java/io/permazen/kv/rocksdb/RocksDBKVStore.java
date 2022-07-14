
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.rocksdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableTracker;

import java.io.Closeable;
import java.util.NoSuchElementException;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link io.permazen.kv.KVStore} view of a RocksDB database.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources associated with iterators.
 */
public class RocksDBKVStore extends AbstractKVStore implements CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final CloseableTracker cursorTracker = new CloseableTracker();
    private final ReadOptions readOptions;
    private final boolean closeReadOptions;
    private final WriteBatch writeBatch;
    private final RocksDB db;

    private volatile boolean closed;

// Constructors

    /**
     * Convenience constructor. Uses default read options and no write batching.
     *
     * @param db database
     */
    public RocksDBKVStore(RocksDB db) {
        this(db, null, null);
    }

    /**
     * Constructor.
     *
     * <p>
     * The caller is responsible for invoking {@link org.rocksdb.RocksObject#close close()} on any supplied
     * {@code readOptions} and/or {@code writeBatch}, after this instance is {@link #close}'d of course.
     *
     * @param db database
     * @param readOptions read options, or null for the default
     * @param writeBatch batch for write operations, or null for none
     * @throws IllegalArgumentException if {@code db} is null
     */
    public RocksDBKVStore(RocksDB db, ReadOptions readOptions, WriteBatch writeBatch) {
        this(db, readOptions != null ? readOptions : new ReadOptions(), readOptions == null, writeBatch);
    }

    RocksDBKVStore(RocksDB db, ReadOptions readOptions, boolean closeReadOptions, WriteBatch writeBatch) {
        Preconditions.checkArgument(db != null, "null db");
        Preconditions.checkArgument(readOptions != null);
        assert RocksDBUtil.isInitialized(db);
        assert RocksDBUtil.isInitialized(readOptions);
        this.db = db;
        this.readOptions = readOptions;
        this.closeReadOptions = closeReadOptions;
        this.writeBatch = writeBatch;
        if (this.log.isTraceEnabled())
            this.log.trace("created {}", this);
    }

// Accessors

    /**
     * Get the {@link RocksDB} underlying this instance.
     *
     * @return underlying database
     */
    public RocksDB getDB() {
        return this.db;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        key.getClass();
        Preconditions.checkState(!this.closed, "closed");
        assert RocksDBUtil.isInitialized(this.db);
        assert RocksDBUtil.isInitialized(this.readOptions);
        this.cursorTracker.poll();
        try {
            return this.db.get(this.readOptions, key);
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB error", e);
        }
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        Preconditions.checkState(!this.closed, "closed");
        return this.createIterator(this.readOptions, minKey, maxKey, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        key.getClass();
        value.getClass();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null) {
            assert RocksDBUtil.isInitialized(this.writeBatch);
            synchronized (this.writeBatch) {
                try {
                    this.writeBatch.put(key, value);
                } catch (RocksDBException e) {
                    throw new RuntimeException("RocksDB error", e);
                }
            }
        } else {
            assert RocksDBUtil.isInitialized(this.db);
            try {
                this.db.put(key, value);
            } catch (RocksDBException e) {
                throw new RuntimeException("RocksDB error", e);
            }
        }
    }

    @Override
    public void remove(byte[] key) {
        key.getClass();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null) {
            assert RocksDBUtil.isInitialized(this.writeBatch);
            synchronized (this.writeBatch) {
                try {
                    this.writeBatch.delete(key);
                } catch (RocksDBException e) {
                    throw new RuntimeException("RocksDB error", e);
                }
            }
        } else {
            assert RocksDBUtil.isInitialized(this.db);
            try {
                this.db.delete(key);
            } catch (RocksDBException e) {
                throw new RuntimeException("RocksDB error", e);
            }
        }
    }

    // RocksDB "uint64add" merge uses little-endian 64-bit counters
    @Override
    public byte[] encodeCounter(long value) {
        final byte[] bytes = new byte[8];
        bytes[0] = (byte)(value >>  0);
        bytes[1] = (byte)(value >>  8);
        bytes[2] = (byte)(value >> 16);
        bytes[3] = (byte)(value >> 24);
        bytes[4] = (byte)(value >> 32);
        bytes[5] = (byte)(value >> 40);
        bytes[6] = (byte)(value >> 48);
        bytes[7] = (byte)(value >> 56);
        return bytes;
    }

    // RocksDB "uint64add" merge uses little-endian 64-bit counters
    @Override
    public long decodeCounter(byte[] value) {
        Preconditions.checkArgument(value.length == 8, "invalid encoded counter value length != 8");
        return ((long)(value[7] & 0xff) << 56)
             | ((long)(value[6] & 0xff) << 48)
             | ((long)(value[5] & 0xff) << 40)
             | ((long)(value[4] & 0xff) << 32)
             | ((long)(value[3] & 0xff) << 24)
             | ((long)(value[2] & 0xff) << 16)
             | ((long)(value[1] & 0xff) <<  8)
             | ((long)(value[0] & 0xff) <<  0);
    }

    // RocksDB "uint64add" merge uses little-endian 64-bit counters
    @Override
    public void adjustCounter(byte[] key, long amount) {
        key.getClass();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        final byte[] value = this.encodeCounter(amount);
        if (this.writeBatch != null) {
            assert RocksDBUtil.isInitialized(this.writeBatch);
            synchronized (this.writeBatch) {
                try {
                    this.writeBatch.merge(key, value);
                } catch (RocksDBException e) {
                    throw new RuntimeException("RocksDB error", e);
                }
            }
        } else {
            assert RocksDBUtil.isInitialized(this.db);
            try {
                this.db.merge(key, value);
            } catch (RocksDBException e) {
                throw new RuntimeException("RocksDB error", e);
            }
        }
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #close} to close any unclosed iterators.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            if (!this.closed)
               this.log.warn(this + " leaked without invoking close()");
            this.close();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[db=" + this.db
          + ",options=" + this.readOptions
          + (this.writeBatch != null ? ",writeBatch=" + this.writeBatch : "")
          + "]";
    }

// Closeable

    /**
     * Close this instance.
     *
     * <p>
     * This closes any unclosed iterators returned from {@link #getRange getRange()}.
     * This does not close the underlying {@link RocksDB} or any associated {@link WriteBatch}.
     */
    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        if (this.log.isTraceEnabled())
            this.log.trace("closing {}", this);
        this.cursorTracker.close();
        if (this.closeReadOptions)
            this.readOptions.close();
    }

// Iterator

    Iterator createIterator(ReadOptions readOptions, byte[] minKey, byte[] maxKey, boolean reverse) {
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        assert RocksDBUtil.isInitialized(this.db);
        assert RocksDBUtil.isInitialized(readOptions);
        //assert readOptions.snapshot() == null || RocksDBUtil.isInitialized(readOptions.snapshot());
        return new Iterator(this.db.newIterator(readOptions), minKey, maxKey, reverse);
    }

    final class Iterator implements CloseableIterator<KVPair> {

        private final RocksIterator cursor;
        private final byte[] minKey;
        private final byte[] maxKey;
        private final boolean reverse;

        private KVPair next;
        private byte[] removeKey;
        private boolean finished;
        private boolean closed;

        private Iterator(final RocksIterator cursor, byte[] minKey, byte[] maxKey, boolean reverse) {

            // Make sure we eventually close the cursor
            RocksDBKVStore.this.cursorTracker.add(this, new CursorCloser(cursor));

            // Sanity checks
            Preconditions.checkArgument(minKey == null || maxKey == null || ByteUtil.compare(minKey, maxKey) <= 0,
              "minKey > maxKey");

            // Initialize
            assert RocksDBUtil.isInitialized(cursor);
            this.cursor = cursor;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.reverse = reverse;
            if (RocksDBKVStore.this.log.isTraceEnabled())
                RocksDBKVStore.this.log.trace("created {}", this);

            // Set initial cursor position
            if (reverse) {
                if (maxKey != null) {
                    if (RocksDBKVStore.this.log.isTraceEnabled())
                        RocksDBKVStore.this.log.trace("seek to {}", ByteUtil.toString(maxKey));
                    assert RocksDBUtil.isInitialized(this.cursor);
                    this.cursor.seek(maxKey);
                    if (this.cursor.isValid()) {
                        if (RocksDBKVStore.this.log.isTraceEnabled())
                            RocksDBKVStore.this.log.trace("valid, seek to previous before {}", ByteUtil.toString(maxKey));
                        assert RocksDBUtil.isInitialized(this.cursor);
                        this.cursor.prev();
                    } else {
                        if (RocksDBKVStore.this.log.isTraceEnabled())
                            RocksDBKVStore.this.log.trace("not valid, seek to last");
                        assert RocksDBUtil.isInitialized(this.cursor);
                        this.cursor.seekToLast();
                    }
                } else {
                    if (RocksDBKVStore.this.log.isTraceEnabled())
                        RocksDBKVStore.this.log.trace("seek to last");
                    assert RocksDBUtil.isInitialized(this.cursor);
                    this.cursor.seekToLast();
                }
            } else {
                if (minKey != null) {
                    if (RocksDBKVStore.this.log.isTraceEnabled())
                        RocksDBKVStore.this.log.trace("seek to {}", ByteUtil.toString(minKey));
                    assert RocksDBUtil.isInitialized(this.cursor);
                    this.cursor.seek(minKey);
                } else {
                    if (RocksDBKVStore.this.log.isTraceEnabled())
                        RocksDBKVStore.this.log.trace("seek to first");
                    assert RocksDBUtil.isInitialized(this.cursor);
                    this.cursor.seekToFirst();
                }
            }
            if (RocksDBKVStore.this.log.isTraceEnabled()) {
                assert RocksDBUtil.isInitialized(this.cursor);
                RocksDBKVStore.this.log.trace("starting position is "
                  + (this.cursor.isValid() ? new KVPair(this.cursor.key(), this.cursor.value()) : "INVALID"));
            }
            assert !this.cursor.isValid() || (this.reverse ?
              maxKey == null || ByteUtil.compare(this.cursor.key(), maxKey) < 0 :
              minKey != null && ByteUtil.compare(this.cursor.key(), minKey) >= 0) :
              "first key " + ByteUtil.toString(this.cursor.key())
              + (reverse ? " >= " + ByteUtil.toString(maxKey) + " max key" : " < " + ByteUtil.toString(minKey) + " min key");

            // Update from cursor
            this.updateFromCursor();
        }

    // Iterator

        @Override
        public synchronized boolean hasNext() {
            Preconditions.checkState(!this.closed, "closed");
            return this.next != null || this.findNext();
        }

        @Override
        public synchronized KVPair next() {
            Preconditions.checkState(!this.closed, "closed");
            if (this.next == null && !this.findNext())
                throw new NoSuchElementException();
            assert this.next != null;
            final KVPair pair = this.next;
            this.removeKey = pair.getKey();
            this.next = null;
            return pair;
        }

        @Override
        public synchronized void remove() {
            Preconditions.checkState(!this.closed, "closed");
            Preconditions.checkState(this.removeKey != null);
            if (RocksDBKVStore.this.log.isTraceEnabled())
                RocksDBKVStore.this.log.trace("remove {}", ByteUtil.toString(this.removeKey));
            RocksDBKVStore.this.remove(this.removeKey);
            this.removeKey = null;
        }

        private boolean findNext() {

            // Sanity check
            assert Thread.holdsLock(this);
            assert !this.closed;
            assert this.next == null;
            if (this.finished)
                return false;

            // Advance cursor
            if (this.reverse) {
                assert RocksDBUtil.isInitialized(this.cursor);
                this.cursor.prev();
                if (RocksDBKVStore.this.log.isTraceEnabled()) {
                    assert RocksDBUtil.isInitialized(this.cursor);
                    RocksDBKVStore.this.log.trace("seek previous -> "
                      + (this.cursor.isValid() ? new KVPair(this.cursor.key(), this.cursor.value()) : "START"));
                }
            } else {
                assert RocksDBUtil.isInitialized(this.cursor);
                this.cursor.next();
                if (RocksDBKVStore.this.log.isTraceEnabled()) {
                    assert RocksDBUtil.isInitialized(this.cursor);
                    RocksDBKVStore.this.log.trace("seek next -> "
                      + (this.cursor.isValid() ? new KVPair(this.cursor.key(), this.cursor.value()) : "END"));
                }
            }

            // Update from cursor
            return this.updateFromCursor();
        }

        private boolean updateFromCursor() {

            // Have we run off the end?
            assert RocksDBUtil.isInitialized(this.cursor);
            if (!this.cursor.isValid()) {
                this.finished = true;
                return false;
            }

            // Read cursor
            assert RocksDBUtil.isInitialized(this.cursor);
            final byte[] key = this.cursor.key();
            assert RocksDBUtil.isInitialized(this.cursor);
            final byte[] value = this.cursor.value();

            // Have we reached our bound?
            if (this.reverse ?
              (this.minKey != null && ByteUtil.compare(key, this.minKey) < 0) :
              (this.maxKey != null && ByteUtil.compare(key, this.maxKey) >= 0)) {
                if (RocksDBKVStore.this.log.isTraceEnabled())
                    RocksDBKVStore.this.log.trace("stop at bound {}", ByteUtil.toString(this.reverse ? this.minKey : this.maxKey));
                this.finished = true;
                return false;
            }

            // Next key/value pair is valid
            this.next = new KVPair(key, value);
            return true;
        }

    // Closeable

        @Override
        public synchronized void close() {
            if (this.closed)
                return;
            this.closed = true;
            if (RocksDBKVStore.this.log.isTraceEnabled())
                RocksDBKVStore.this.log.trace("closing {}", this);
            new CursorCloser(this.cursor).close();
        }

    // Object

        @Override
        public String toString() {
            return RocksDBKVStore.class.getSimpleName() + "." + this.getClass().getSimpleName()
              + "[minKey=" + ByteUtil.toString(this.minKey)
              + ",maxKey=" + ByteUtil.toString(this.maxKey)
              + (this.reverse ? ",reverse" : "")
              + "]";
        }
    }

// CursorCloser

    // This needs to be a static class so it doesn't have a strong reference to the Iterator that created it
    private static class CursorCloser implements Closeable {

        private final RocksIterator cursor;

        CursorCloser(RocksIterator cursor) {
            this.cursor = cursor;
        }

        @Override
        public void close() {

            // RocksDB objects implement AutoCloseable, not Closeable, so we have to close them exactly once
            synchronized (this.cursor) {
                if (RocksDBUtil.isInitialized(this.cursor)) {
                    try {
                        this.cursor.close();
                    } catch (Throwable e) {
                        LoggerFactory.getLogger(this.getClass()).debug("caught exception closing db iterator (ignoring)", e);
                    }
                }
            }
        }
    }
}

