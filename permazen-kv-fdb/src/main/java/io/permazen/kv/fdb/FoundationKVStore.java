
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link KVStore} view of a FoundationDB {@link Transaction}.
 */
@ThreadSafe
public class FoundationKVStore implements KVStore {

    private static final byte[] MIN_KEY = ByteUtil.EMPTY;                   // minimum possible key (inclusive)
    private static final byte[] MAX_KEY = new byte[] { (byte)0xff };        // maximum possible key (exclusive)

    private final Transaction tx;
    private final byte[] keyPrefix;

    /**
     * Constructor.
     *
     * @param tx FDB transaction; note, caller is responsible for closing this
     * @param keyPrefix key prefix, or null for none
     */
    public FoundationKVStore(Transaction tx, byte[] keyPrefix) {
        Preconditions.checkArgument(tx != null, "null tx");
        this.tx = tx;
        this.keyPrefix = keyPrefix != null ? keyPrefix.clone() : null;
    }

    /**
     * Get the underlying {@link Transaction} associated with this instance.
     *
     * @return the associated transaction
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    /**
     * Get the key prefix associated with this instance, if any.
     *
     * @return the associated key prefix, or null for none
     */
    public byte[] getKeyPrefix() {
        return this.keyPrefix != null ? this.keyPrefix.clone() : null;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        try {
            return this.tx.get(this.addPrefix(key)).get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException ? (RuntimeException)e.getCause() : new RuntimeException(e.getCause());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            return null;
        return this.getFirstInRange(minKey, maxKey, false);
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        return this.getFirstInRange(minKey, maxKey, true);
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            minKey = MAX_KEY;
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        Preconditions.checkArgument(minKey == null || maxKey == null || ByteUtil.compare(minKey, maxKey) <= 0, "minKey > maxKey");
        final AsyncIterator<KeyValue> i = this.tx.getRange(
          this.addPrefix(minKey, maxKey), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
        return CloseableIterator.wrap(
          Iterators.transform(i, kv -> new KVPair(this.removePrefix(kv.getKey()), kv.getValue())),
          i instanceof AutoCloseable ? (AutoCloseable)i : (AutoCloseable)i::cancel);
    }

    private KVPair getFirstInRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        final AsyncIterator<KeyValue> i = this.tx.getRange(
          this.addPrefix(minKey, maxKey), ReadTransaction.ROW_LIMIT_UNLIMITED /* 1? */, reverse).iterator();
        try {
            if (!i.hasNext())
                return null;
            final KeyValue kv = i.next();
            return new KVPair(this.removePrefix(kv.getKey()), kv.getValue());
        } finally {
            i.cancel();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        this.tx.set(this.addPrefix(key), value);
    }

    @Override
    public void remove(byte[] key) {
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        this.tx.clear(this.addPrefix(key));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        if (minKey != null && minKey.length > 0 && minKey[0] == (byte)0xff)
            return;
        if (maxKey != null && maxKey.length > 0 && maxKey[0] == (byte)0xff)
            maxKey = null;
        Preconditions.checkArgument(minKey == null || maxKey == null || ByteUtil.compare(minKey, maxKey) <= 0, "minKey > maxKey");
        this.tx.clear(this.addPrefix(minKey, maxKey));
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
        this.tx.mutate(MutationType.ADD, this.addPrefix(key), this.encodeCounter(amount));
    }

// Key prefixing

    byte[] addPrefix(byte[] key) {
        return this.keyPrefix != null ? Bytes.concat(this.keyPrefix, key) : key;
    }

    Range addPrefix(byte[] minKey, byte[] maxKey) {
        if (this.keyPrefix == null && maxKey != null && maxKey.length > 1 && maxKey[0] == (byte)0xff)
            maxKey = MAX_KEY;
        return new Range(this.addPrefix(minKey != null ? minKey : MIN_KEY), this.addPrefix(maxKey != null ? maxKey : MAX_KEY));
    }

    byte[] removePrefix(byte[] key) {
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
