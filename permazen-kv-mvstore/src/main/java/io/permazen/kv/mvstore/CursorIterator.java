
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteUtil;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;

/**
 * Forward {@link KVPair} iterator based on an underlying {@link Cursor}.
 */
public class CursorIterator extends AbstractIterator {

    private final Cursor<byte[], byte[]> cursor;
    private final byte[] maxKey;

// Constructors

    /**
     * Constructor for a read-only instance.
     *
     * <p>
     * Attempts to invoke {@link #remove} will result in {@link UnsupportedOperationException}.
     *
     * @param cursor the underlying {@link Cursor}
     * @param maxKey maximum key (exclusive), or null for no maximum (end at the largest key)
     * @throws IllegalArgumentException if {@code cursor} is null
     */
    public CursorIterator(Cursor<byte[], byte[]> cursor, byte[] maxKey) {
        this(null, cursor, maxKey);
    }

    /**
     * Constructor.
     *
     * @param mvmap the underlying {@link MVMap} (used to implement {@link #remove}), or null for read-only operation
     * @param cursor the underlying {@link Cursor}
     * @param maxKey maximum key (exclusive), or null for no maximum (end at the largest key)
     * @throws IllegalArgumentException if {@code cursor} is null
     */
    public CursorIterator(MVMap<byte[], byte[]> mvmap, Cursor<byte[], byte[]> cursor, byte[] maxKey) {
        super(mvmap);
        Preconditions.checkArgument(cursor != null, "null cursor");
        this.cursor = cursor;
        this.maxKey = maxKey;
    }

// AbstractIterator

    @Override
    protected KVPair findNext() {
        return this.cursor.hasNext() ? new KVPair(this.cursor.next(), this.cursor.getValue()) : null;
    }

    @Override
    protected boolean boundsCheck(byte[] key) {
        return this.maxKey == null || ByteUtil.compare(key, this.maxKey) < 0;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[mvmap=" + this.mvmap
          + "]";
    }
}
