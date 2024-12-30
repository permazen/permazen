
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.fallback;

import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;

import java.util.ArrayList;

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
    public FallbackKVDatabase getKVDatabase() {
        return this.db;
    }

    @Override
    public void setTimeout(long timeout) {
        this.kvt.setTimeout(timeout);
    }

    @Override
    public ListenableFuture<Void> watchKey(ByteData key) {

        // Check freshness
        synchronized (this.db) {
            if (this.stale)
                throw new StaleKVTransactionException(this);
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
    public CloseableKVStore readOnlySnapshot() {
        return this.kvt.readOnlySnapshot();
    }

    @Override
    public boolean isReadOnly() {
        return this.kvt.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.kvt.setReadOnly(readOnly);
    }

    @Override
    public void commit() {

        // Check freshness
        synchronized (this.db) {
            if (this.stale)
                throw new StaleKVTransactionException(this);
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
            throw new RetryKVTransactionException(this, "fallback migration in progress");
        }
    }
}
