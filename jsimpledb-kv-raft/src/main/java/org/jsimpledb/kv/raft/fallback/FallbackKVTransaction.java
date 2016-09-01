
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.fallback;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;

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

    private ArrayList<FallbackKVDatabase.FallbackFuture> futureList;

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
    public ListenableFuture<Void> watchKey(byte[] key) {

        // Check freshness
        synchronized (this.db) {
            if (this.stale)
                throw new StaleTransactionException(this);
        }

        // Get target's future - it must be a ListenableFuture or we can't do this
        final ListenableFuture<Void> innerFuture;
        try {
            innerFuture = (ListenableFuture<Void>)this.kvt.watchKey(key);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("nested transaction does not support ListenableFuture's", e);
        }

        // Create outer future
        final FallbackKVDatabase.FallbackFuture outerFuture = this.db.new FallbackFuture(innerFuture);

        // Add outer future to our pending list
        synchronized (this) {
            if (this.futureList == null)
                this.futureList = new ArrayList<>();
            this.futureList.add(outerFuture);
        }

        // Done
        return outerFuture;
    }

    @Override
    public CloseableKVStore mutableSnapshot() {
        return this.kvt.mutableSnapshot();
    }

    @Override
    public void commit() {

        // Check freshness
        synchronized (this.db) {
            if (this.stale)
                throw new StaleTransactionException(this);
            this.stale = true;
        }

        // Check to see if migration occurred
        this.retryIfMigrating();

        // Commit nested transaction
        this.kvt.commit();

        // Snapshot our pending futures (if any)
        final ArrayList<FallbackKVDatabase.FallbackFuture> fallbackFutures;
        synchronized (this) {
            fallbackFutures = this.futureList;
            this.futureList = null;
        }

        // Check again to see if migration occurred - this is required to close
        // a race where the database could appear to revert to an earlier version
        this.retryIfMigrating();

        // Register futures, but if they are already out-of-date, trigger spurious notifications instead
        if (fallbackFutures != null && !this.db.registerFallbackFutures(fallbackFutures, this.migrationCount)) {
            for (FallbackKVDatabase.FallbackFuture future : fallbackFutures)
                future.set(null);
        }
    }

    @Override
    public void rollback() {
        synchronized (this.db) {
            this.stale = true;
            this.kvt.rollback();
        }
    }

    private void retryIfMigrating() {
        if (!this.db.checkNoMigration(this.migrationCount)) {
            this.rollback();
            throw new RetryTransactionException(this, "fallback migration in progress");
        }
    }
}

