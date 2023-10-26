
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteUtil;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link KVStore} view of an {@link MVMap}.
 */
public class MVMapKVStore extends AbstractKVStore {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final MVMap<byte[], byte[]> mvmap;

// Constructors

    /**
     * Default constructor.
     *
     * <p>
     * If this constructor is used, this class must be subclassed and {@link #getMVMap}
     * overridden to supply the delegate {@link MVMap}.
     *
     * @throws IllegalArgumentException if {@code mvmap} is null
     */
    protected MVMapKVStore() {
        this.mvmap = null;
    }

    /**
     * Constructor.
     *
     * @param mvmap the underlying {@link MVMap} to use
     * @throws IllegalArgumentException if {@code mvmap} is null
     */
    public MVMapKVStore(MVMap<byte[], byte[]> mvmap) {
        Preconditions.checkArgument(mvmap != null, "null mvmap");
        this.mvmap = mvmap;
    }

    /**
     * Get the underlying {@link MVMap} associated with this instance.
     *
     * <p>
     * The implementation in {@link MVMapKVStore} returns the {@link MVMap} passed to the constructor, if any.
     * Subclasses that use this class' default constructor must override this method.
     *
     * @return underlying {@link MVMap}
     */
    public MVMap<byte[], byte[]> getMVMap() {
        return this.mvmap;
    }

// KVStore

    @Override
    public byte[] get(byte[] key) {
        return this.getMVMap().get(key);
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        while (true) {
            final byte[] key = minKey != null ? this.getMVMap().ceilingKey(minKey) : this.getMVMap().firstKey();
            if (key == null || (maxKey != null && ByteUtil.compare(key, maxKey) >= 0))
                return null;
            final byte[] value = this.getMVMap().get(key);
            if (value != null)
                return new KVPair(key, value);
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        while (true) {
            final byte[] key = maxKey != null ? this.getMVMap().lowerKey(maxKey) : this.getMVMap().lastKey();
            if (key == null || (minKey != null && ByteUtil.compare(key, minKey) < 0))
                return null;
            final byte[] value = this.getMVMap().get(key);
            if (value != null)
                return new KVPair(key, value);
        }
    }

    @Override
    public CursorIterator getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        return new CursorIterator(this.getMVMap(), minKey, maxKey, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (this.getMVMap().isReadOnly())
            throw new UnsupportedOperationException("MVMap is read-only");
        this.getMVMap().put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        if (this.getMVMap().isReadOnly())
            throw new UnsupportedOperationException("MVMap is read-only");
        this.getMVMap().remove(key);
    }

//    See https://github.com/h2database/h2database/issues/2002
//    @Override
//    public void removeRange(byte[] minKey, byte[] maxKey) {
//    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[mvmap=" + this.getMVMap()
          + "]";
    }
}
