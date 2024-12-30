
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link KVStore} view of an {@link MVMap}.
 */
public class MVMapKVStore extends AbstractKVStore {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final MVMap<ByteData, ByteData> mvmap;

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
    public MVMapKVStore(MVMap<ByteData, ByteData> mvmap) {
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
    public MVMap<ByteData, ByteData> getMVMap() {
        return this.mvmap;
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        return this.getMVMap().get(key);
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        while (true) {
            final ByteData key = minKey != null ? this.getMVMap().ceilingKey(minKey) : this.getMVMap().firstKey();
            if (key == null || (maxKey != null && key.compareTo(maxKey) >= 0))
                return null;
            final ByteData value = this.getMVMap().get(key);
            if (value != null)
                return new KVPair(key, value);
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        while (true) {
            final ByteData key = maxKey != null ? this.getMVMap().lowerKey(maxKey) : this.getMVMap().lastKey();
            if (key == null || (minKey != null && key.compareTo(minKey) < 0))
                return null;
            final ByteData value = this.getMVMap().get(key);
            if (value != null)
                return new KVPair(key, value);
        }
    }

    @Override
    public CursorIterator getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return new CursorIterator(this.getMVMap(), minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        if (this.getMVMap().isReadOnly())
            throw new UnsupportedOperationException("MVMap is read-only");
        this.getMVMap().put(key, value);
    }

    @Override
    public void remove(ByteData key) {
        if (this.getMVMap().isReadOnly())
            throw new UnsupportedOperationException("MVMap is read-only");
        this.getMVMap().remove(key);
    }

//    See https://github.com/h2database/h2database/issues/2002
//    @Override
//    public void removeRange(ByteData minKey, ByteData maxKey) {
//    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[mvmap=" + this.getMVMap()
          + "]";
    }
}
