
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
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.google.common.primitives.Bytes;

import org.jsimpledb.kv.CountingKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * FoundationDB transaction.
 */
public class FoundationKVTransaction implements KVTransaction, CountingKVStore {

    private static final byte[] MIN_KEY = new byte[0];                      // minimum possible key (inclusive)
    private static final byte[] MAX_KEY = new byte[] { (byte)0xff };        // maximum possible key (exclusive)

    private final FoundationKVDatabase store;
    private final Transaction tx;
    private final byte[] keyPrefix;

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
            return this.tx.get(this.addPrefix(key)).get();
        } catch (FDBException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            return null;
        return this.getFirstInRange(minKey, null, false);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        return this.getFirstInRange(null, maxKey, true);
    }

    private KVPair getFirstInRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        try {
            final AsyncIterator<KeyValue> i = this.tx.getRange(this.addPrefix(minKey, maxKey), 1, reverse).iterator();
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

// CountingKVStore

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
        if (bytes.length != 8)
            throw new IllegalArgumentException("invalid encoded counter value: length = " + bytes.length + " != 8");
        bytes = bytes.clone();
        this.reverse(bytes);
        return ByteUtil.readLong(new ByteReader(bytes));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
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

