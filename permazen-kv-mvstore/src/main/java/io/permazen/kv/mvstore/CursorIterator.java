
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;

/**
 * {@link KVPair} iterator based on an underlying {@link Cursor}.
 *
 * <p>
 * The {@link #remove} method is implemented by invoking {@link MVMap#remove(Object)}.
 */
@ThreadSafe
public class CursorIterator implements CloseableIterator<KVPair> {

    private final MVMap<ByteData, ByteData> mvmap;
    private final MVCursorIterator iterator;

    @GuardedBy("this")
    private ByteData lastKey;

// Constructors

    /**
     * Constructor.
     *
     * @param mvmap the underlying {@link MVMap} (used to implement {@link #remove}), or null for read-only operation
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @param reverse true if {@code cursor} iterates keys in descending order
     * @throws IllegalArgumentException if {@code cursor} is null
     */
    public CursorIterator(MVMap<ByteData, ByteData> mvmap, ByteData minKey, ByteData maxKey, boolean reverse) {
        Preconditions.checkArgument(mvmap != null, "null mvmap");
        this.mvmap = mvmap;
        this.iterator = reverse ?
          new MVCursorIterator(this.mvmap.cursor(maxKey, minKey, true), maxKey, reverse) :
          new MVCursorIterator(this.mvmap.cursor(minKey, maxKey, false), maxKey, reverse);
    }

    /**
     * Get the {@link MVMap} underlying this instance, if any.
     *
     * @return underlying {@link MVMap}, or null if none provided
     */
    public MVMap<ByteData, ByteData> getMVMap() {
        return this.mvmap;
    }

// Iterator

    @Override
    public synchronized KVPair next() {
        final KVPair kv = this.iterator.next();
        this.lastKey = kv.getKey();
        return kv;
    }

    @Override
    public synchronized boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public synchronized void remove() {
        Preconditions.checkState(this.lastKey != null);
        this.mvmap.remove(this.lastKey);
        this.lastKey = null;
    }

// Closeable

    @Override
    public void close() {
        // nothing to do
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[mvmap=" + this.mvmap
          + ",iterator=" + this.iterator
          + "]";
    }
}
