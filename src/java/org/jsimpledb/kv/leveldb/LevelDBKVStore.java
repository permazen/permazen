
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.CloseableTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AtomicKVStore} view of a LevelDB database.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources associated with iterators.
 * </p>
 */
public class LevelDBKVStore extends AbstractKVStore implements AtomicKVStore, CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final CloseableTracker cursorTracker = new CloseableTracker();
    private final ReadOptions readOptions;
    private final WriteBatch writeBatch;
    private final DB db;

    private boolean closed;

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
    public LevelDBKVStore(DB db, ReadOptions readOptions, WriteBatch writeBatch) {
        if (db == null)
            throw new IllegalArgumentException("null db");
        this.db = db;
        this.readOptions = readOptions != null ? readOptions : new ReadOptions();
        this.writeBatch = writeBatch;
        if (this.log.isTraceEnabled())
            this.log.trace("created " + this);
    }

// Accessors

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
    public synchronized byte[] get(byte[] key) {
        key.getClass();
        if (this.closed)
            throw new IllegalStateException("the store is closed");
        this.cursorTracker.poll();
        return this.db.get(key, this.readOptions);
    }

    @Override
    public synchronized java.util.Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (this.closed)
            throw new IllegalStateException("the store is closed");
        this.cursorTracker.poll();
        return new Iterator(this.db.iterator(this.readOptions), minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        key.getClass();
        value.getClass();
        if (this.closed)
            throw new IllegalStateException("the store is closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null)
            this.writeBatch.put(key, value);
        else
            this.db.put(key, value);
    }

    @Override
    public synchronized void remove(byte[] key) {
        key.getClass();
        if (this.closed)
            throw new IllegalStateException("the store is closed");
        this.cursorTracker.poll();
        if (this.writeBatch != null)
            this.writeBatch.delete(key);
        else
            this.db.delete(key);
    }

// AtomicKVStore

    @Override
    public synchronized CloseableKVStore snapshot() {
        return new SnapshotLevelDBKVStore(this.db, this.readOptions.verifyChecksums());
    }

    @Override
    public synchronized void mutate(Mutations mutations, boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Apply mutations in a batch
        try (WriteBatch batch = this.db.createWriteBatch()) {

            // Apply removes
            final ReadOptions iteratorOptions = new ReadOptions()
              .verifyChecksums(this.readOptions.verifyChecksums())
              .snapshot(this.readOptions.snapshot())
              .fillCache(false);
            for (KeyRange range : mutations.getRemoveRanges()) {
                final byte[] min = range.getMin();
                final byte[] max = range.getMax();
                if (min != null && max != null && ByteUtil.compare(max, ByteUtil.getNextKey(min)) == 0)
                    batch.delete(min);
                else {
                    try (Iterator i = new Iterator(this.db.iterator(iteratorOptions), min, max, false)) {
                        while (i.hasNext())
                            batch.delete(i.next().getKey());
                    }
                }
            }

            // Apply puts
            for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs())
                batch.put(entry.getKey(), entry.getValue());

            // Convert counter adjustments into puts
            final Function<Map.Entry<byte[], Long>, Map.Entry<byte[], byte[]>> counterPutFunction
              = new Function<Map.Entry<byte[], Long>, Map.Entry<byte[], byte[]>>() {
                @Override
                public Map.Entry<byte[], byte[]> apply(Map.Entry<byte[], Long> adjust) {

                    // Decode old value
                    final byte[] key = adjust.getKey();
                    final long diff = adjust.getValue();
                    byte[] oldBytes = LevelDBKVStore.this.db.get(key, LevelDBKVStore.this.readOptions);
                    if (oldBytes == null)
                        oldBytes  = new byte[8];
                    final long oldValue;
                    try {
                        oldValue = LevelDBKVStore.this.decodeCounter(oldBytes);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }

                    // Add adjustment and re-encode it
                    return new AbstractMap.SimpleEntry<byte[], byte[]>(key, LevelDBKVStore.this.encodeCounter(oldValue + diff));
                }
            };

            // Apply counter adjustments
            for (Map.Entry<byte[], byte[]> entry : Iterables.transform(mutations.getAdjustPairs(), counterPutFunction)) {
                if (entry != null)
                    batch.put(entry.getKey(), entry.getValue());
            }

            // Write the batch
            this.db.write(batch, new WriteOptions().sync(sync));
        } catch (IOException e) {
            throw new DBException("error applying changes to LevelDB", e);
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
     * This does not close the underlying {@link DB} or any associated {@link WriteBatch}.
     */
    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        if (this.log.isTraceEnabled())
            this.log.trace("closing " + this);
        this.cursorTracker.close();
    }

// Iterator

    private final class Iterator implements java.util.Iterator<KVPair>, Closeable {

        private final DBIterator cursor;
        private final byte[] minKey;
        private final byte[] maxKey;
        private final boolean reverse;

        private KVPair next;
        private byte[] removeKey;
        private boolean finished;
        private boolean closed;

        Iterator(DBIterator cursor, byte[] minKey, byte[] maxKey, boolean reverse) {

            // Make sure we eventually close the iterator
            LevelDBKVStore.this.cursorTracker.add(this, cursor);

            // Sanity checks
            assert Thread.holdsLock(LevelDBKVStore.this);
            if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
                throw new IllegalArgumentException("minKey > maxKey");

            // Initialize
            this.cursor = cursor;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.reverse = reverse;
            if (LevelDBKVStore.this.log.isTraceEnabled())
                LevelDBKVStore.this.log.trace("created " + this);
            if (reverse) {
                if (maxKey != null) {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to " + ByteUtil.toString(maxKey));
                    this.cursor.seek(maxKey);
                } else {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to last");
                    this.cursor.seekToLast();
                }
            } else {
                if (minKey != null) {
                    if (LevelDBKVStore.this.log.isTraceEnabled())
                        LevelDBKVStore.this.log.trace("seek to " + ByteUtil.toString(minKey));
                    this.cursor.seek(minKey);
                }
            }

            // Debug
        }

    // Iterator

        @Override
        public synchronized boolean hasNext() {
            if (this.closed)
                throw new IllegalStateException();
            return this.next != null || this.findNext();
        }

        @Override
        public synchronized KVPair next() {
            if (this.closed)
                throw new IllegalStateException();
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
            if (this.closed || this.removeKey == null)
                throw new IllegalStateException();
            if (LevelDBKVStore.this.log.isTraceEnabled())
                LevelDBKVStore.this.log.trace("remove " + ByteUtil.toString(this.removeKey));
            LevelDBKVStore.this.remove(this.removeKey);
            this.removeKey = null;
        }

        private synchronized boolean findNext() {

            // Sanity check
            assert this.next == null;
            if (this.finished)
                return false;

            // Advance LevelDB cursor
            try {
                this.next = new KVPair(this.reverse ? this.cursor.prev() : this.cursor.next());
                if (LevelDBKVStore.this.log.isTraceEnabled())
                    LevelDBKVStore.this.log.trace("seek " + (this.reverse ? "previous" : "next") + " -> " + this.next);
            } catch (NoSuchElementException e) {
                if (LevelDBKVStore.this.log.isTraceEnabled())
                    LevelDBKVStore.this.log.trace("seek " + (this.reverse ? "previous" : "next") + " -> NO MORE");
                this.finished = true;
                return false;
            }

            // Check limit bound
            if (this.reverse ?
              (this.minKey != null && ByteUtil.compare(this.next.getKey(), this.minKey) < 0) :
              (this.maxKey != null && ByteUtil.compare(this.next.getKey(), this.maxKey) >= 0)) {
                if (LevelDBKVStore.this.log.isTraceEnabled()) {
                    LevelDBKVStore.this.log.trace("stopping at bound "
                      + ByteUtil.toString(this.reverse ? this.minKey : this.maxKey));
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
                LevelDBKVStore.this.log.trace("closing " + this);
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

