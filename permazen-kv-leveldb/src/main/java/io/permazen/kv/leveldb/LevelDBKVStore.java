
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableTracker;

import java.util.NoSuchElementException;
import java.util.Optional;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link KVStore} view of a LevelDB database.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources associated with iterators.
 */
public class LevelDBKVStore extends AbstractKVStore implements CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final CloseableTracker cursorTracker = new CloseableTracker();
    private final ReadOptions readOptions;
    private final WriteBatch writeBatch;
    private final DB db;

    private volatile boolean closed;

// Constructors

    /**
     * Convenience constructor. Uses default read options and no write batching.
     *
     * @param db database
     */
    public LevelDBKVStore(DB db) {
        this(db, null, null);
    }

    /**
     * Constructor.
     *
     * @param db database
     * @param readOptions read options, or null for the default
     * @param writeBatch batch for write operations, or null for none
     * @throws IllegalArgumentException if {@code db} is null
     */
    @SuppressWarnings("this-escape")
    public LevelDBKVStore(DB db, ReadOptions readOptions, WriteBatch writeBatch) {
        Preconditions.checkArgument(db != null, "null db");
        this.db = db;
        this.readOptions = readOptions != null ? readOptions : new ReadOptions();
        this.writeBatch = writeBatch;
        if (this.log.isTraceEnabled())
            this.log.trace("created {}", this);
    }

    /**
     * Get the {@link DB} underlying this instance.
     *
     * @return underlying database
     */
    public DB getDB() {
        return this.db;
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        final byte[] keyBytes = key.toByteArray();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        return Optional.ofNullable(this.db.get(keyBytes, this.readOptions))
          .map(ByteData::of)
          .orElse(null);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return this.createIterator(this.readOptions, minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        final byte[] keyBytes = key.toByteArray();
        final byte[] valueBytes = value.toByteArray();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null) {
            synchronized (this.writeBatch) {
                this.writeBatch.put(keyBytes, valueBytes);
            }
        } else
            this.db.put(keyBytes, valueBytes);
    }

    @Override
    public void remove(ByteData key) {
        final byte[] keyBytes = key.toByteArray();
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null) {
            synchronized (this.writeBatch) {
                this.writeBatch.delete(keyBytes);
            }
        } else
            this.db.delete(keyBytes);
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #close} to close any unclosed iterators.
     */
    @Override
    @SuppressWarnings("deprecation")
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
     * This does not close the underlying {@link DB} or any associated {@link WriteBatch}.
     */
    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        if (this.log.isTraceEnabled())
            this.log.trace("closing {}", this);
        this.cursorTracker.close();
    }

// Iterator

    Iterator createIterator(ReadOptions readOptions, ByteData minKey, ByteData maxKey, boolean reverse) {
        Preconditions.checkState(!this.closed, "closed");
        this.cursorTracker.poll();
        return new Iterator(this.db.iterator(readOptions), minKey, maxKey, reverse);
    }

    final class Iterator implements CloseableIterator<KVPair> {

        private final DBIterator cursor;
        private final ByteData minKey;
        private final ByteData maxKey;
        private final boolean reverse;

        private KVPair next;
        private ByteData removeKey;
        private boolean finished;
        private boolean closed;

        private Iterator(DBIterator cursor, ByteData minKey, ByteData maxKey, boolean reverse) {

            // Make sure we eventually close the iterator
            LevelDBKVStore.this.cursorTracker.add(this, cursor);

            // Sanity checks
            Preconditions.checkArgument(minKey == null || maxKey == null || minKey.compareTo(maxKey) <= 0, "minKey > maxKey");

            // Initialize
            this.cursor = cursor;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.reverse = reverse;
            if (LevelDBKVStore.this.log.isTraceEnabled())
                LevelDBKVStore.this.log.trace("created {}", this);
            if (reverse) {
                if (maxKey != null) {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to {}", ByteUtil.toString(maxKey));
                    this.cursor.seek(maxKey.toByteArray());
                } else {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to last");
                    this.cursor.seekToLast();
                }
            } else {
                if (minKey != null) {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to {}", ByteUtil.toString(minKey));
                    this.cursor.seek(minKey.toByteArray());
                }
            }
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
            if (LevelDBKVStore.this.log.isTraceEnabled())
                LevelDBKVStore.this.log.trace("remove {}", ByteUtil.toString(this.removeKey));
            LevelDBKVStore.this.remove(this.removeKey);
            this.removeKey = null;
        }

        private boolean findNext() {

            // Sanity check
            assert this.next == null;
            if (this.finished)
                return false;

            // Advance LevelDB cursor
            try {
                this.next = new KVPair(this.reverse ? this.cursor.prev() : this.cursor.next());
                if (LevelDBKVStore.this.log.isTraceEnabled())
                    LevelDBKVStore.this.log.trace("seek {} -> {}", this.reverse ? "previous" : "next", this.next);
            } catch (NoSuchElementException e) {
                if (LevelDBKVStore.this.log.isTraceEnabled())
                    LevelDBKVStore.this.log.trace("seek {} -> {}", this.reverse ? "previous" : "next", "NO MORE");
                this.finished = true;
                return false;
            }

            // Check limit bound
            if (this.reverse ?
              (this.minKey != null && this.next.getKey().compareTo(this.minKey) < 0) :
              (this.maxKey != null && this.next.getKey().compareTo(this.maxKey) >= 0)) {
                if (LevelDBKVStore.this.log.isTraceEnabled()) {
                    LevelDBKVStore.this.log.trace("stopping at bound {}",
                      ByteUtil.toString(this.reverse ? this.minKey : this.maxKey));
                }
                this.next = null;
                this.finished = true;
                return false;
            }

            // Done
            return true;
        }

    // Closeable

        @Override
        public synchronized void close() {
            if (this.closed)
                return;
            this.closed = true;
            if (LevelDBKVStore.this.log.isTraceEnabled())
                LevelDBKVStore.this.log.trace("closing {}", this);
            try {
                this.cursor.close();
            } catch (Throwable e) {
                LevelDBKVStore.this.log.debug("caught exception closing db iterator (ignoring)", e);
            }
        }

    // Object

        @Override
        public String toString() {
            return LevelDBKVStore.class.getSimpleName() + "." + this.getClass().getSimpleName()
              + "[minKey=" + ByteUtil.toString(this.minKey)
              + ",maxKey=" + ByteUtil.toString(this.maxKey)
              + (this.reverse ? ",reverse" : "")
              + "]";
        }
    }
}
