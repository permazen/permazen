
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
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.util.ByteUtil;

/**
 * Transaction created by a {@link PrefixKVDatabase}.
 */
public class PrefixKVTransaction implements KVTransaction {

    private final PrefixKVDatabase db;
    private final byte[] keyPrefix;
    private final KVTransaction tx;

    /**
     * Constructor.
     *
     * @param db the containing {@link PrefixKVDatabase}
     * @throws IllegalArgumentException if {@code db} is null
     */
    protected PrefixKVTransaction(PrefixKVDatabase db) {
        if (db == null)
            throw new IllegalArgumentException("null db");
        this.db = db;
        this.keyPrefix = db.getKeyPrefix();
        this.tx = this.db.createTransaction();
    }

// KVTransaction

    @Override
    public PrefixKVDatabase getKVDatabase() {
        return this.db;
    }

    @Override
    public byte[] get(byte[] key) {
        return this.tx.get(this.addPrefix(key));
    }

    @Override
    public KVPair getAtLeast(byte[] minKey) {
        final KVPair pair = this.tx.getAtLeast(this.addMinPrefix(minKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public KVPair getAtMost(byte[] maxKey) {
        final KVPair pair = this.tx.getAtMost(this.addMaxPrefix(maxKey));
        return new KVPair(this.removePrefix(pair.getKey()), pair.getValue());
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        final Iterator<KVPair> i = this.tx.getRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey), reverse);
        return Iterators.transform(i, new Function<KVPair, KVPair>() {
            @Override
            public KVPair apply(KVPair pair) {
                return new KVPair(PrefixKVTransaction.this.removePrefix(pair.getKey()), pair.getValue());
            }
        });
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.tx.put(this.addPrefix(key), value);
    }

    @Override
    public void remove(byte[] key) {
        this.tx.remove(this.addPrefix(key));
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.tx.removeRange(this.addMinPrefix(minKey), this.addMaxPrefix(maxKey));
    }

    @Override
    public void setTimeout(long timeout) {
        this.tx.setTimeout(timeout);
    }

    @Override
    public void commit() {
        this.tx.commit();
    }

    @Override
    public void rollback() {
        this.tx.rollback();
    }

    @Override
    public byte[] encodeCounter(long value) {
        return this.tx.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return this.tx.decodeCounter(bytes);
    }

    @Override
    public void adjustCounter(byte[] key, long amount) {
        this.tx.adjustCounter(this.addPrefix(key), amount);
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

