
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.ByteUtil;

/**
 * A {@link KVStore} view of all keys having a common {@code byte[]} prefix in a containing {@link KVStore}.
 */
public class PrefixKVStore extends ForwardingKVStore {

    private final byte[] keyPrefix;

    /**
     * Constructor.
     *
     * @param kvstore the containing {@link KVStore}
     * @param keyPrefix prefix for all keys
     * @throws IllegalArgumentException if {@code kvstore} or {@code keyPrefix} is null
     */
    public PrefixKVStore(KVStore kvstore, byte[] keyPrefix) {
        super(kvstore);
        if (keyPrefix == null)
            throw new IllegalStateException("null keyPrefix");
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
     * Get the containing {@link KVStore} associated with this instance.
     *
     * @return the containing {@link KVStore}
     */
    public KVStore getContainingKVStore() {
        return this.kvstore;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        return this.kvstore.get(this.addPrefix(key));
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        final KVPair pair = this.kvstore.getAtLeast(this.addMinPrefix(minKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        final KVPair pair = this.kvstore.getAtMost(this.addMaxPrefix(maxKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        final Iterator<KVPair> i = this.kvstore.getRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey), reverse);
        return Iterators.transform(i, new Function<KVPair, KVPair>() {
            @Override
            public KVPair apply(KVPair pair) {
                return new KVPair(PrefixKVStore.this.removePrefix(pair.getKey()), pair.getValue());
            }
        });
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.kvstore.put(this.addPrefix(key), value);
    }

    @Override
    public void remove(byte[] key) {
        this.kvstore.remove(this.addPrefix(key));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.kvstore.removeRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey));
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        this.kvstore.adjustCounter(this.addPrefix(key), amount);
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

