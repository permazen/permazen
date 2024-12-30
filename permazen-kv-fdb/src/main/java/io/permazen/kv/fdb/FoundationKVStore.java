
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

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link KVStore} view of a FoundationDB {@link Transaction}.
 */
@ThreadSafe
public class FoundationKVStore implements KVStore {

    static final ByteData MIN_KEY = ByteData.empty();               // minimum possible key (inclusive)
    static final ByteData MAX_KEY = ByteData.of(0xff);              // maximum possible key (exclusive)

    private final Transaction tx;
    private final ByteData keyPrefix;

    /**
     * Constructor.
     *
     * @param tx FDB transaction; note, caller is responsible for closing this
     * @param keyPrefix key prefix, or null or empty for none
     */
    public FoundationKVStore(Transaction tx, ByteData keyPrefix) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(keyPrefix == null || !keyPrefix.startsWith(MAX_KEY), "prefix starts with 0xff");
        this.tx = tx;
        if (keyPrefix != null && keyPrefix.isEmpty())
            keyPrefix = null;
        this.keyPrefix = keyPrefix;
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
     * @return the associated key prefix, or null or empty for none
     */
    public ByteData getKeyPrefix() {
        return this.keyPrefix;
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        Preconditions.checkArgument(!key.startsWith(MAX_KEY), "key starts with 0xff");
        try {
            return ByteData.of(this.tx.get(this.addPrefix(key).toByteArray()).get());
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException ? (RuntimeException)e.getCause() : new RuntimeException(e.getCause());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        if (minKey != null && minKey.startsWith(MAX_KEY))
            return null;
        return this.getFirstInRange(minKey, maxKey, false);
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        if (maxKey != null && maxKey.startsWith(MAX_KEY))
            maxKey = null;
        return this.getFirstInRange(minKey, maxKey, true);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        if (minKey != null && minKey.startsWith(MAX_KEY))
            minKey = MAX_KEY;
        if (maxKey != null && maxKey.startsWith(MAX_KEY))
            maxKey = null;
        Preconditions.checkArgument(minKey == null || maxKey == null || minKey.compareTo(maxKey) <= 0, "minKey > maxKey");
        final AsyncIterator<KeyValue> i = this.tx.getRange(
          this.buildRange(minKey, maxKey), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator();
        return CloseableIterator.wrap(
          Iterators.transform(i, kv -> new KVPair(this.removePrefix(ByteData.of(kv.getKey())), ByteData.of(kv.getValue()))),
          i instanceof AutoCloseable ? (AutoCloseable)i : (AutoCloseable)i::cancel);
    }

    private KVPair getFirstInRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        final AsyncIterator<KeyValue> i = this.tx.getRange(
          this.buildRange(minKey, maxKey), ReadTransaction.ROW_LIMIT_UNLIMITED /* 1? */, reverse).iterator();
        try {
            if (!i.hasNext())
                return null;
            final KeyValue kv = i.next();
            return new KVPair(this.removePrefix(ByteData.of(kv.getKey())), ByteData.of(kv.getValue()));
        } finally {
            i.cancel();
        }
    }

    @Override
    public void put(ByteData key, ByteData value) {
        Preconditions.checkArgument(!key.startsWith(MAX_KEY), "key starts with 0xff");
        this.tx.set(this.addPrefix(key).toByteArray(), value.toByteArray());
    }

    @Override
    public void remove(ByteData key) {
        Preconditions.checkArgument(!key.startsWith(MAX_KEY), "key starts with 0xff");
        this.tx.clear(this.addPrefix(key).toByteArray());
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        if (minKey != null && minKey.startsWith(MAX_KEY))
            return;
        if (maxKey != null && maxKey.startsWith(MAX_KEY))
            maxKey = null;
        Preconditions.checkArgument(minKey == null || maxKey == null || minKey.compareTo(maxKey) <= 0, "minKey > maxKey");
        this.tx.clear(this.buildRange(minKey, maxKey));
    }

    @Override
    public ByteData encodeCounter(long value) {
        return FoundationKVDatabase.encodeCounter(value);
    }

    @Override
    public long decodeCounter(ByteData bytes) {
        return FoundationKVDatabase.decodeCounter(bytes);
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        this.tx.mutate(MutationType.ADD, this.addPrefix(key).toByteArray(), this.encodeCounter(amount).toByteArray());
    }

// Key prefixing

    ByteData addPrefix(ByteData key) {
        return this.keyPrefix != null ? this.keyPrefix.concat(key) : key;
    }

    Range buildRange(ByteData minKey, ByteData maxKey) {
        if (this.keyPrefix == null && maxKey != null && maxKey.startsWith(MAX_KEY))
            maxKey = MAX_KEY;
        final byte[] rangeMin = this.addPrefix(minKey != null ? minKey : MIN_KEY).toByteArray();
        final byte[] rangeMax = this.addPrefix(maxKey != null ? maxKey : MAX_KEY).toByteArray();
        return new Range(rangeMin, rangeMax);
    }

    ByteData removePrefix(ByteData key) {
        if (this.keyPrefix == null)
            return key;
        if (!key.startsWith(this.keyPrefix)) {
            throw new IllegalArgumentException(String.format(
              "read key %s not having %s as a prefix", ByteUtil.toString(key), ByteUtil.toString(this.keyPrefix)));
        }
        return key.substring(this.keyPrefix.size());
    }
}
