
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;

import org.h2.mvstore.MVMap;

/**
 * Reverse {@link KVPair} iterator on an underlying {@link MVMap}.
 *
 * <p>
 * This is an inefficient algorithm that invokes {@link MVMap#lowerKey} for each element.
 *
 * @see <a href="https://github.com/h2database/h2database/issues/2000">Feature request: MVStore descending cursors (issue #2000)</a>
 */
public class ReverseIterator extends AbstractIterator {

    private byte[] minKey;
    private byte[] anchor;                                      // the (exclusive) upper bound for the next downward search

// Constructors

    /**
     * Constructor.
     *
     * @param mvmap the underlying {@link MVMap}
     * @param maxKey maximum key (exclusive), or null for no maximum (start at the largest key)
     * @param minKey minimum key (inclusive), or null for no minimum (end at the smallest key)
     * @throws IllegalArgumentException if {@code mvmap} is null
     */
    public ReverseIterator(MVMap<byte[], byte[]> mvmap, byte[] maxKey, byte[] minKey) {
        super(mvmap);
        Preconditions.checkArgument(mvmap != null, "null mvmap");
        this.minKey = minKey;
        this.anchor = maxKey;
    }

// AbstractIterator

    @Override
    protected KVPair findNext() {
        while (true) {
            final byte[] key = this.anchor != null ? this.mvmap.lowerKey(this.anchor) : this.mvmap.lastKey();
            if (key == null)
                return null;
            this.anchor = key;
            final byte[] value = this.mvmap.get(key);
            if (value != null)                          // otherwise somebody else is concurrently writing to the map, so try again
                return new KVPair(key, value);
        }
    }

    @Override
    protected boolean boundsCheck(byte[] key) {
        return this.minKey == null || ByteUtil.compare(key, this.minKey) >= 0;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[mvmap=" + this.getMVMap()
          + "]";
    }
}
