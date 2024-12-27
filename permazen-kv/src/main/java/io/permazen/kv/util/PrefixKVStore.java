
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

/**
 * A {@link KVStore} view of all keys having a common {@code byte[]} prefix in an outer, containing {@link KVStore}.
 */
public abstract class PrefixKVStore extends ForwardingKVStore {

    private final ByteData keyPrefix;

    /**
     * Constructor.
     *
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code keyPrefix} is null
     */
    public PrefixKVStore(ByteData keyPrefix) {
        Preconditions.checkArgument(keyPrefix != null, "null keyPrefix");
        this.keyPrefix = keyPrefix;
    }

    /**
     * Get the {@code byte[]} key prefix associated with this instance.
     *
     * @return this instance's key prefix
     */
    public final ByteData getKeyPrefix() {
        return this.keyPrefix;
    }

    /**
     * Create a {@link PrefixKVStore} instance using the specified prefix and underlying {@link KVStore}.
     *
     * @param kvstore underyling key/value store
     * @param keyPrefix prefix for all keys
     * @return view of all keys in {@code kvstore} with prefix {@code keyPrefix}
     * @throws IllegalArgumentException if either parameter is null
     */
    public static PrefixKVStore create(final KVStore kvstore, ByteData keyPrefix) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(keyPrefix != null, "null keyPrefix");
        return new PrefixKVStore(keyPrefix) {
            @Override
            protected KVStore delegate() {
                return kvstore;
            }
        };
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        return this.delegate().get(this.addPrefix(key));
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        final KVPair pair = this.delegate().getAtLeast(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey));
        if (pair == null)
            return null;
        assert pair.getKey().startsWith(this.keyPrefix);
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        final KVPair pair = this.delegate().getAtMost(this.addMaxPrefix(maxKey), this.addMinPrefix(minKey));
        if (pair == null)
            return null;
        assert pair.getKey().startsWith(this.keyPrefix);
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        final CloseableIterator<KVPair> i = this.delegate().getRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey), reverse);
        return CloseableIterator.wrap(
          Iterators.transform(i, pair -> new KVPair(this.removePrefix(pair.getKey()), pair.getValue())), i);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        this.delegate().put(this.addPrefix(key), value);
    }

    @Override
    public void remove(ByteData key) {
        this.delegate().remove(this.addPrefix(key));
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        this.delegate().removeRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey));
    }

    @Override
    public void adjustCounter(ByteData key, long amount) {
        this.delegate().adjustCounter(this.addPrefix(key), amount);
    }

// Key (un)prefixing

    private ByteData addPrefix(ByteData key) {
        if (key == null)
            return null;
        return this.keyPrefix.concat(key);
    }

    private ByteData addMinPrefix(ByteData minKey) {
        if (minKey == null)
            return this.keyPrefix;
        return this.addPrefix(minKey);
    }

    private ByteData addMaxPrefix(ByteData maxKey) {
        if (maxKey == null)
            return !this.keyPrefix.isEmpty() ? ByteUtil.getKeyAfterPrefix(this.keyPrefix) : null;
        return this.addPrefix(maxKey);
    }

    private ByteData removePrefix(ByteData key) {
        if (!key.startsWith(this.keyPrefix)) {
            throw new IllegalArgumentException(String.format(
              "read key %s not having %s as a prefix", ByteUtil.toString(key), ByteUtil.toString(this.keyPrefix)));
        }
        return key.substring(this.keyPrefix.size());
    }
}
