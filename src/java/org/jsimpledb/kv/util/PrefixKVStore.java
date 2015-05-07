
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.ByteUtil;

/**
 * A {@link org.jsimpledb.kv.KVStore} view of all keys having a common {@code byte[]} prefix
 * in an outer, containing {@link org.jsimpledb.kv.KVStore}.
 */
public abstract class PrefixKVStore extends ForwardingKVStore {

    private final byte[] keyPrefix;

    /**
     * Constructor.
     *
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code keyPrefix} is null
     */
    public PrefixKVStore(byte[] keyPrefix) {
        Preconditions.checkArgument(keyPrefix != null, "null keyPrefix");
        this.keyPrefix = keyPrefix.clone();
    }

    /**
     * Get the {@code byte[]} key prefix associated with this instance.
     *
     * @return (a copy of) this instance's key prefix
     */
    public final byte[] getKeyPrefix() {
        return this.keyPrefix.clone();
    }

    /**
     * Create a {@link PrefixKVStore} instance using the specified prefix and underlying {@link KVStore}.
     *
     * @param kvstore underyling key/value store
     * @param keyPrefix prefix for all keys
     * @return view of all keys in {@code kvstore} with prefix {@code keyPrefix}
     * @throws IllegalArgumentException if either parameter is null
     */
    public static PrefixKVStore create(final KVStore kvstore, byte[] keyPrefix) {
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
    public byte[] get(byte[] key) {
        return this.delegate().get(this.addPrefix(key));
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        final KVPair pair = this.delegate().getAtLeast(this.addMinPrefix(minKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        final KVPair pair = this.delegate().getAtMost(this.addMaxPrefix(maxKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        final Iterator<KVPair> i = this.delegate().getRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey), reverse);
        return Iterators.transform(i, new Function<KVPair, KVPair>() {
            @Override
            public KVPair apply(KVPair pair) {
                return new KVPair(PrefixKVStore.this.removePrefix(pair.getKey()), pair.getValue());
            }
        });
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.delegate().put(this.addPrefix(key), value);
    }

    @Override
    public void remove(byte[] key) {
        this.delegate().remove(this.addPrefix(key));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.delegate().removeRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        this.delegate().adjustCounter(this.addPrefix(key), amount);
    }

// Key (un)prefixing

    private byte[] addPrefix(byte[] key) {
        return Bytes.concat(this.keyPrefix, key);
    }

    private byte[] addMinPrefix(byte[] minKey) {
        if (minKey == null)
            return this.keyPrefix.clone();
        return this.addPrefix(minKey);
    }

    private byte[] addMaxPrefix(byte[] maxKey) {
        if (maxKey == null)
            return this.keyPrefix.length > 0 ? ByteUtil.getKeyAfterPrefix(this.keyPrefix) : null;
        return this.addPrefix(maxKey);
    }

    private byte[] removePrefix(byte[] key) {
        if (!ByteUtil.isPrefixOf(this.keyPrefix, key)) {
            throw new IllegalArgumentException("read key " + ByteUtil.toString(key) + " not having "
              + ByteUtil.toString(this.keyPrefix) + " as a prefix");
        }
        final byte[] suffix = new byte[key.length - this.keyPrefix.length];
        System.arraycopy(key, this.keyPrefix.length, suffix, 0, suffix.length);
        return suffix;
    }
}

