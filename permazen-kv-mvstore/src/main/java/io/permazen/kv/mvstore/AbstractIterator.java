
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.CloseableIterator;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.h2.mvstore.MVMap;

/**
 * Support superclass for {@link KVPair} iterators reading from on an underlying {@link MVMap}.
 *
 * <p>
 * The {@link #remove} method is implemented by invoking {@link MVMap#remove(Object)}.
 */
@ThreadSafe
abstract class AbstractIterator implements CloseableIterator<KVPair> {

    protected final MVMap<byte[], byte[]> mvmap;

    private final com.google.common.collect.AbstractIterator<KVPair> iter;

    @GuardedBy("this")
    private byte[] lastKey;

// Constructor

    protected AbstractIterator(MVMap<byte[], byte[]> mvmap) {
        this.mvmap = mvmap;
        this.iter = new com.google.common.collect.AbstractIterator<KVPair>() {
            @Override
            protected KVPair computeNext() {
                final KVPair kv = AbstractIterator.this.findNext();
                return kv != null && AbstractIterator.this.boundsCheck(kv.getKey()) ? kv : this.endOfData();
            }
        };
    }

    protected abstract KVPair findNext();

    protected abstract boolean boundsCheck(byte[] key);

    /**
     * Get the {@link MVMap} underlying this instance, if any.
     *
     * @return underlying {@link MVMap}, or null if none provided
     */
    public MVMap<byte[], byte[]> getMVMap() {
        return this.mvmap;
    }

// Iterator

    @Override
    public synchronized KVPair next() {
        final KVPair kv = this.iter.next();
        this.lastKey = kv.getKey();
        return kv;
    }

    @Override
    public synchronized boolean hasNext() {
        return this.iter.hasNext();
    }

    @Override
    public synchronized void remove() {
        if (this.mvmap == null)
            throw new UnsupportedOperationException();
        Preconditions.checkState(this.lastKey != null);
        this.mvmap.remove(this.lastKey);
        this.lastKey = null;
    }

// Closeable

    @Override
    public void close() {
        // nothing to do
    }
}
