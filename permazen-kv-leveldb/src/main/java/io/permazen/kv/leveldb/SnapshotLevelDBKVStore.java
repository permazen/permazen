
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only {@link KVStore} view of a LevelDB {@link Snapshot}.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources associated with iterators.
 * This class ensures that the configured {@link Snapshot} is closed when this instance is closed.
 *
 * <p>
 * All mutation operations throw {@link UnsupportedOperationException}.
 */
public class SnapshotLevelDBKVStore extends LevelDBKVStore implements CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Snapshot snapshot;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param db LevelDB database to snapshot
     * @param verifyChecksums whether to verify checksums on reads
     * @throws NullPointerException if {@code db} is null
     */
    public SnapshotLevelDBKVStore(DB db, boolean verifyChecksums) {
        this(db, db.getSnapshot(), verifyChecksums);
    }

    private SnapshotLevelDBKVStore(DB db, Snapshot snapshot, boolean verifyChecksums) {
        super(db, new ReadOptions().snapshot(snapshot).verifyChecksums(verifyChecksums), null);
        this.snapshot = snapshot;
    }

    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        super.close();
        try {
            this.snapshot.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing LevelDB snapshot (ignoring)", e);
        }
    }

// KVStore

    @Override
    public void put(ByteData key, ByteData value) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void remove(ByteData key) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        throw new UnsupportedOperationException("KVStore is read-only");
    }
}
