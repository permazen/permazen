
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import org.jsimpledb.kv.CloseableKVStore;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only {@link org.jsimpledb.kv.KVStore} view of a RocksDB {@link Snapshot}.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources associated with iterators.
 * This class ensures that the configured {@link Snapshot} is closed when this instance is closed.
 *
 * <p>
 * All mutation operations throw {@link UnsupportedOperationException}.
 */
public class SnapshotRocksDBKVStore extends RocksDBKVStore implements CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Snapshot snapshot;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param db RocksDB database to snapshot
     * @throws NullPointerException if {@code db} is null
     */
    public SnapshotRocksDBKVStore(RocksDB db) {
        this(db, db.getSnapshot());
    }

    private SnapshotRocksDBKVStore(RocksDB db, Snapshot snapshot) {
        super(db, new ReadOptions().setSnapshot(snapshot), true, null);
        this.snapshot = snapshot;
    }

// Closeable

    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        try {
            this.snapshot.dispose();
        } catch (Throwable e) {
            this.log.error("caught exception closing RocksDB snapshot (ignoring)", e);
        }
        super.close();
    }

// KVStore

    @Override
    public void put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void remove(byte[] key) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }
}

