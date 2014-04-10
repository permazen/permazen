
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.fdb;

import com.foundationdb.FDBException;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;

/**
 * FoundationDB transaction.
 */
public class FoundationKVTransaction implements KVTransaction {

    private static final byte[] MIN_KEY = new byte[0];                      // minimum possible key (inclusive)
    private static final byte[] MAX_KEY = new byte[] { (byte)0xff };        // maximum possible key (exclusive)

    private final FoundationKVDatabase store;
    private final Transaction tx;

    /**
     * Constructor.
     */
    FoundationKVTransaction(FoundationKVDatabase store) {
        if (store == null)
            throw new IllegalArgumentException("null store");
        this.store = store;
        this.tx = this.store.getDatabase().createTransaction();
    }

    @Override
    public FoundationKVDatabase getKVDatabase() {
        return this.store;
    }

    /**
     * Get the underlying {@link Transaction} associated with this instance.
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    @Override
    public void setTimeout(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        this.tx.options().setTimeout(timeout);
    }

    @Override
    public byte[] get(byte[] key) {
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            return this.tx.get(key).get();
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        return this.getRange(KeySelector.firstGreaterOrEqual(minKey != null ? minKey : MIN_KEY),
          KeySelector.lastLessThan(MAX_KEY), false);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        return this.getRange(KeySelector.firstGreaterOrEqual(MIN_KEY),
          KeySelector.lastLessThan(maxKey != null ? maxKey : MAX_KEY), true);
    }

    private KVPair getRange(KeySelector min, KeySelector max, boolean reverse) {
        try {
            final AsyncIterator<KeyValue> i = this.tx.getRange(min, max, 1).iterator();
            if (!i.hasNext())
                return null;
            final KeyValue kv = i.next();
            return new KVPair(kv.getKey(), kv.getValue());
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            this.tx.set(key, value);
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void remove(byte[] key) {
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            this.tx.clear(key);
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        try {
            this.tx.clear(minKey != null ? minKey : MIN_KEY, maxKey != null ? maxKey : MAX_KEY);
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void commit() {
        try {
            this.tx.commit().get();
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            this.tx.cancel();
        } catch (FDBException e) {
            throw this.wrapException(e);
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
        switch (e.getCode()) {
        case ErrorCodes.TRANSACTION_TIMED_OUT:
            return new TransactionTimeoutException(this, e);
        case ErrorCodes.NOT_COMMITTED:
        case ErrorCodes.COMMIT_UNKNOWN_RESULT:
            return new RetryTransactionException(this, e);
        default:
            return new KVTransactionException(this, e);
        }
    }
}

