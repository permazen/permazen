
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link XodusKVDatabase} transaction.
 */
@ThreadSafe
public class XodusKVTransaction extends ForwardingKVStore implements KVTransaction {

    // Lock order: (1) XodusKVTransaction, (2) XodusKVDatabase

    private final XodusKVDatabase kvdb;

    @GuardedBy("this")
    private XodusKVStore kv;
    @GuardedBy("this")
    private KVStore delegate;
    @GuardedBy("this")
    private TransactionType transactionType = TransactionType.READ_WRITE;
    @GuardedBy("this")
    private boolean closed;

    /**
     * Constructor.
     */
    XodusKVTransaction(XodusKVDatabase kvdb) {
        this.kvdb = kvdb;
    }

// KVTransaction

    @Override
    public XodusKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public synchronized void commit() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        this.kvdb.transactionClosed(this);
        this.closed = true;
        if (this.kv != null) {
            if (!this.kv.close(!this.transactionType.isReadOnly()))
                throw new RetryKVTransactionException(this);
        }
    }

    @Override
    public synchronized void rollback() {
        if (this.closed)
            return;
        this.kvdb.transactionClosed(this);
        this.closed = true;
        if (this.kv != null)
            this.kv.close(false);
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.transactionType.isReadOnly();
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        Preconditions.checkState(this.kv == null, "already accessed");
        this.transactionType = readOnly ? TransactionType.READ_ONLY : TransactionType.READ_WRITE;
    }

    @Override
    public synchronized CloseableKVStore readOnlySnapshot() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        return this.buildKV().readOnlySnapshot();
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

    private synchronized XodusKVStore buildKV() {
        if (this.closed)
            throw new StaleKVTransactionException(this, "transaction closed");
        if (this.kv == null) {
            assert this.delegate == null;
            this.kv = new XodusKVStore(this.kvdb.getEnvironment(), this.kvdb.getStoreName(), this.transactionType);
            this.delegate = this.transactionType.isReadOnly() ? new MutableView(this.kv, false) : this.kv;
        }
        return this.kv;
    }
}
