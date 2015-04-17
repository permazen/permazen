
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import java.io.Closeable;
import java.util.HashSet;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.util.UnmodifiableKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One LevelDB {@link KVDatabase} MVCC version.
 */
class VersionInfo implements Closeable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final long version;
    private final HashSet<LevelDBKVTransaction> openTransactions = new HashSet<>(2);
    private final Snapshot snapshot;
    private final LevelDBKVStore snapshotKV;

    private LevelDBKVTransaction committedTransaction;
    private boolean closed;

    /**
     * Constructor.
     *
     * @param version version number
     * @param snapshot database snapshot
     */
    VersionInfo(DB db, long version, Snapshot snapshot, boolean verifyChecksums) {
        this.version = version;
        this.snapshot = snapshot;
        this.snapshotKV = new LevelDBKVStore(db, new ReadOptions().snapshot(this.snapshot).verifyChecksums(verifyChecksums), null);
    }

    /**
     * Get this instance's unique version number.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Get the {@link KVStore} view of this version's snapshot.
     *
     * @return unmodifiable {@link KVStore}
     */
    public KVStore getSnapshot() {
        return new UnmodifiableKVStore(this.snapshotKV);
    }

    /**
     * Get transactions based on this version's snapshot that are still open.
     */
    public HashSet<LevelDBKVTransaction> getOpenTransactions() {
        return this.openTransactions;
    }

    /**
     * Get the transaction based on this version's snapshot that was eventually committed, if any.
     */
    public LevelDBKVTransaction getCommittedTransaction() {
        return this.committedTransaction;
    }
    public void setCommittedTransaction(LevelDBKVTransaction committedTransaction) {
        this.committedTransaction = committedTransaction;
    }

// Closeable

    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        this.snapshotKV.close();
        try {
            this.snapshot.close();
        } catch (Throwable e) {
            this.log.error("caught exception closing database snapshot (ignoring)", e);
        }
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[vers=" + this.version
          + ",snapshot=" + this.snapshot
          + ",openTx=" + this.openTransactions
          + ",commitTx=" + this.committedTransaction
          + (this.closed ? ",closed" : "")
          + "]";
    }
}

