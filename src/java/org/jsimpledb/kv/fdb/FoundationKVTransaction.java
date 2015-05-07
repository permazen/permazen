
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.fdb;

import com.foundationdb.FDBException;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * FoundationDB transaction.
 */
public class FoundationKVTransaction implements KVTransaction {

    private static final byte[] MIN_KEY = ByteUtil.EMPTY;                   // minimum possible key (inclusive)
    private static final byte[] MAX_KEY = new byte[] { (byte)0xff };        // maximum possible key (exclusive)

    private final FoundationKVDatabase store;
    private final Transaction tx;
    private final byte[] keyPrefix;

    private volatile boolean stale;
    private volatile boolean canceled;

    /**
     * Constructor.
     */
    FoundationKVTransaction(FoundationKVDatabase store, byte[] keyPrefix) {
        if (store == null)
            throw new IllegalArgumentException("null store");
        this.store = store;
        this.tx = this.store.getDatabase().createTransaction();
        this.keyPrefix = keyPrefix;
    }

// KVTransaction

    @Override
    public FoundationKVDatabase getKVDatabase() {
        return this.store;
    }

    /**
     * Get the underlying {@link Transaction} associated with this instance.
     *
     * @return the associated transaction
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
        if (this.stale)
            throw new StaleTransactionException(this);
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            return this.tx.get(this.addPrefix(key)).get();
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            return null;
        return this.getFirstInRange(minKey, null, false);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        return this.getFirstInRange(null, maxKey, true);
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            minKey = MAX_KEY;
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
            throw new IllegalArgumentException("minKey > maxKey");
        try {
            return Iterators.transform(this.tx.getRange(this.addPrefix(minKey, maxKey),
              ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator(), new Function<KeyValue, KVPair>() {
                @Override
                public KVPair apply(KeyValue kv) {
                    return new KVPair(FoundationKVTransaction.this.removePrefix(kv.getKey()), kv.getValue());
                }
            });
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    private KVPair getFirstInRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        try {
            final AsyncIterator<KeyValue> i = this.tx.getRange(this.addPrefix(minKey, maxKey),
              ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
            if (!i.hasNext())
                return null;
            final KeyValue kv = i.next();
            return new KVPair(this.removePrefix(kv.getKey()), kv.getValue());
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            this.tx.set(this.addPrefix(key), value);
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void remove(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        try {
            this.tx.clear(this.addPrefix(key));
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            return;
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
            throw new IllegalArgumentException("minKey > maxKey");
        try {
            this.tx.clear(this.addPrefix(minKey, maxKey));
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public void commit() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        try {
            this.tx.commit().get();
        } catch (FDBException e) {
            throw this.wrapException(e);
        } finally {
            this.cancel();
        }
    }

    @Override
    public void rollback() {
        if (this.stale)
            return;
        this.stale = true;
        this.cancel();
    }

    private void cancel() {
        if (this.canceled)
            return;
        this.canceled = true;
        try {
            this.tx.cancel();
        } catch (FDBException e) {
            // ignore
        }
    }

    @Override
    public byte[] encodeCounter(long value) {
        final ByteWriter writer = new ByteWriter(8);
        ByteUtil.writeLong(writer, value);
        final byte[] bytes = writer.getBytes();
        this.reverse(bytes);
        return bytes;
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (bytes.length != 8)
            throw new IllegalArgumentException("invalid encoded counter value: length = " + bytes.length + " != 8");
        bytes = bytes.clone();
        this.reverse(bytes);
        return ByteUtil.readLong(new ByteReader(bytes));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.tx.mutate(MutationType.ADD, this.addPrefix(key), this.encodeCounter(amount));
    }

    private void reverse(byte[] bytes) {
        int i = 0;
        int j = bytes.length - 1;
        while (i < j) {
            final byte temp = bytes[i];
            bytes[i] = bytes[j];
            bytes[j] = temp;
            i++;
            j--;
        }
    }

// Other methods

    /**
     * Wrap the given {@link FDBException} in the appropriate {@link KVTransactionException}.
     *
     * @param e FoundationDB exception
     * @return appropriate {@link KVTransactionException} with chained exception {@code e}
     * @throws NullPointerException if {@code e} is null
     */
    public KVTransactionException wrapException(FDBException e) {
        try {
            this.cancel();
        } catch (KVTransactionException e2) {
            // ignore
        }
        switch (e.getCode()) {
        case ErrorCodes.TRANSACTION_TIMED_OUT:
        case ErrorCodes.PAST_VERSION:
            return new TransactionTimeoutException(this, e);
        case ErrorCodes.NOT_COMMITTED:
        case ErrorCodes.COMMIT_UNKNOWN_RESULT:
            return new RetryTransactionException(this, e);
        default:
            return new KVTransactionException(this, e);
        }
    }

// Key prefixing

    private byte[] addPrefix(byte[] key) {
        return this.keyPrefix != null ? Bytes.concat(this.keyPrefix, key) : key;
    }

    private Range addPrefix(byte[] minKey, byte[] maxKey) {
        return new Range(this.addPrefix(minKey != null ? minKey : MIN_KEY), this.addPrefix(maxKey != null ? maxKey : MAX_KEY));
    }

    private byte[] removePrefix(byte[] key) {
        if (this.keyPrefix == null)
            return key;
        if (!ByteUtil.isPrefixOf(this.keyPrefix, key)) {
            throw new IllegalArgumentException("read key " + ByteUtil.toString(key) + " not having "
              + ByteUtil.toString(this.keyPrefix) + " as a prefix");
        }
        final byte[] stripped = new byte[key.length - this.keyPrefix.length];
        System.arraycopy(key, this.keyPrefix.length, stripped, 0, stripped.length);
        return stripped;
    }
}

