
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.fallback;

import java.util.concurrent.Future;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.util.ForwardingKVStore;

/**
 * A {@link KVTransaction} associated with a {@link FallbackKVDatabase}.
 *
 * @see FallbackKVDatabase
 */
public class FallbackKVTransaction extends ForwardingKVStore implements KVTransaction {

    private final FallbackKVDatabase db;
    private final KVTransaction kvt;
    private final int migrationCount;

    private boolean stale;              // protected by this.db's monitor

    FallbackKVTransaction(FallbackKVDatabase db, KVTransaction kvt, int migrationCount) {
        this.db = db;
        this.kvt = kvt;
        this.migrationCount = migrationCount;
    }

// ForwardingKVStore

    /**
     * Get the underlying transaction, which will be associated with either one of the fallback
     * databases, or the standalone mode database.
     *
     * @return underlying transaction
     */
    public KVTransaction getKVTransaction() {
        return this.kvt;
    }

    @Override
    protected KVStore delegate() {
        return this.kvt;
    }

// KVTransaction

    @Override
    public KVDatabase getKVDatabase() {
        return this.db;
    }

    @Override
    public void setTimeout(long timeout) {
        this.kvt.setTimeout(timeout);
    }

    @Override
    public Future<Void> watchKey(byte[] key) {
        return this.kvt.watchKey(key);
    }

    @Override
    public CloseableKVStore mutableSnapshot() {
        return this.kvt.mutableSnapshot();
    }

    @Override
    public void commit() {
        synchronized (this.db) {
            if (this.stale)
                throw new StaleTransactionException(this);
            this.stale = true;
        }
        this.retryIfMigrating();
        this.kvt.commit();
        this.retryIfMigrating();    // required to close a window where database would appear to revert to an earlier version
    }

    @Override
    public void rollback() {
        synchronized (this.db) {
            this.stale = true;
            this.kvt.rollback();
        }
    }

    private void retryIfMigrating() {
        synchronized (this.db) {
            if (this.db.isMigrating() || this.migrationCount != this.db.getMigrationCount())
                throw new RetryTransactionException(this, "fallback migration in progress");
        }
    }
}

