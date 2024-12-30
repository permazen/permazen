
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.lmdb;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

/**
 * LMDB transaction viewed as a {@link KVTransaction}.
 *
 * @param <T> buffer type
 */
@ThreadSafe
public abstract class LMDBKVTransaction<T> extends ForwardingKVStore implements KVTransaction {

    // Lock order: (1) LMDBKVTransaction, (2) LMDBKVDatabase

    private final LMDBKVDatabase<T> kvdb;
    private final Env<T> env;
    private final Dbi<T> db;

    @GuardedBy("this")
    private Txn<T> tx;
    @GuardedBy("this")
    private LMDBKVStore<T> kv;
    @GuardedBy("this")
    private KVStore delegate;
    @GuardedBy("this")
    private boolean readOnly;
    @GuardedBy("this")
    private boolean closed;

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param env environment
     * @param db database handle
     */
    protected LMDBKVTransaction(LMDBKVDatabase<T> kvdb, Env<T> env, Dbi<T> db) {
        this.kvdb = kvdb;
        this.env = env;
        this.db = db;
    }

// KVTransaction

    @Override
    public LMDBKVDatabase<T> getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public synchronized void commit() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        this.kvdb.transactionClosed(this);
        this.closed = true;
        if (this.kv != null) {
            this.kv.close();
            this.tx.commit();
            this.tx = null;
        }
    }

    @Override
    public synchronized void rollback() {
        if (this.closed)
            return;
        this.kvdb.transactionClosed(this);
        this.closed = true;
        if (this.kv != null) {
            this.kv.close();
            this.tx.abort();
            this.tx = null;
        }
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        Preconditions.checkState(this.kv == null || readOnly == this.readOnly, "already accessed");
        this.readOnly = readOnly;
    }

    @Override
    public synchronized CloseableKVStore readOnlySnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException("setTimeout() not supported");
    }

    @Override
    public Future<Void> watchKey(ByteData key) {
        throw new UnsupportedOperationException("watchKey() not supported");
    }

// ForwardingKVStore

    @Override
    protected synchronized KVStore delegate() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        if (this.kv == null)
            this.buildKV();
        return this.delegate;
    }

// Internal methods

    protected abstract LMDBKVStore<T> createKVStore(Dbi<T> db, Txn<T> tx);

    private synchronized LMDBKVStore<T> buildKV() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        if (this.kv == null) {
            assert this.delegate == null;
            this.tx = this.readOnly ? this.env.txnRead() : this.env.txnWrite();
            this.kv = this.createKVStore(this.db, this.tx);
            this.delegate = this.readOnly ? new MutableView(this.kv, false) : this.kv;
        }
        return this.kv;
    }
}
