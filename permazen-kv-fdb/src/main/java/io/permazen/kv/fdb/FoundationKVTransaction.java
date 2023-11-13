
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.StaleTransactionException;
import io.permazen.kv.TransactionTimeoutException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * FoundationDB transaction.
 */
@ThreadSafe
public class FoundationKVTransaction implements KVTransaction {

    private static final byte[] MIN_KEY = ByteUtil.EMPTY;                   // minimum possible key (inclusive)
    private static final byte[] MAX_KEY = new byte[] { (byte)0xff };        // maximum possible key (exclusive)

    private final FoundationKVDatabase kvdb;
    private final FoundationKVStore kvstore;
    private final Transaction tx;

    @GuardedBy("this")
    private boolean readOnly;
    @GuardedBy("this")
    private KVStore view;
    @GuardedBy("this")
    private boolean closed;

    /**
     * Constructor.
     */
    FoundationKVTransaction(FoundationKVDatabase kvdb, Transaction tx, byte[] keyPrefix) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        Preconditions.checkArgument(tx != null, "null tx");
        this.kvdb = kvdb;
        this.tx = tx;
        this.kvstore = new FoundationKVStore(this.tx, keyPrefix);
    }

    /**
     * Get the underlying {@link Transaction} associated with this instance.
     *
     * <p>
     * Note: even if this transaction is read-only, the returned {@link Transaction} will permit mutations
     * that persist on {@link #commit}; use accordingly.
     *
     * @return the associated transaction
     */
    public Transaction getTransaction() {
        return this.tx;
    }

// KVTransaction

    @Override
    public FoundationKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public synchronized void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.tx.options().setTimeout(timeout);
    }

    @Override
    public synchronized CompletableFuture<Void> watchKey(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        if (this.closed)
            throw new StaleTransactionException(this);
        try {
            return this.tx.watch(this.kvstore.addPrefix(key));
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        if (this.closed)
            throw new StaleTransactionException(this);
        Preconditions.checkState(this.view == null || readOnly == this.readOnly, "data already accessed");
        this.readOnly = readOnly;
    }

    @Override
    public synchronized void commit() {
        if (this.closed)
            throw new StaleTransactionException(this);
        try {
            this.tx.commit().get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof FDBException ?
              this.wrapException((FDBException)e.getCause()) : new KVTransactionException(this, e.getCause());
        } catch (InterruptedException e) {
            throw new KVTransactionException(this, e);
        } finally {
            this.close();
        }
    }

    @Override
    public void rollback() {
        this.close();
    }

    @Override
    public CloseableKVStore mutableSnapshot() {
        throw new UnsupportedOperationException();
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        try {
            return this.getKVStore().get(key);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        try {
            return this.getKVStore().getAtLeast(minKey, maxKey);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        try {
            return this.getKVStore().getAtMost(maxKey, minKey);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        try {
            return this.getKVStore().getRange(minKey, maxKey, reverse);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            this.getKVStore().put(key, value);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public void remove(byte[] key) {
        try {
            this.getKVStore().remove(key);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        try {
            this.getKVStore().removeRange(minKey, maxKey);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

    @Override
    public byte[] encodeCounter(long value) {
        return FoundationKVDatabase.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return FoundationKVDatabase.decodeCounter(bytes);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        try {
            this.getKVStore().adjustCounter(key, amount);
        } catch (FDBException e) {
            this.close();
            throw this.wrapException(e);
        }
    }

// Internal methods

    private synchronized void close() {
        if (this.closed)
            return;
        this.closed = true;
        try {
            this.tx.close();
        } catch (FDBException e) {
            // ignore
        }
    }

    /**
     * Wrap the given {@link FDBException} in the appropriate {@link KVTransactionException}.
     *
     * @param e FoundationDB exception
     * @return appropriate {@link KVTransactionException} with chained exception {@code e}
     * @throws NullPointerException if {@code e} is null
     */
    public KVTransactionException wrapException(FDBException e) {
        if (e.getCode() == ErrorCode.TRANSACTION_TIMED_OUT.getCode())
            return new TransactionTimeoutException(this, e);
        if (e.getCode() < 1500)
            return new RetryTransactionException(this, e);
        return new KVTransactionException(this, e);
    }

    private synchronized KVStore getKVStore() {
        if (this.closed)
            throw new StaleTransactionException(this);
        if (this.view == null)
            this.view = this.readOnly ? new MutableView(this.kvstore, false) : this.kvstore;
        return this.view;
    }
}
