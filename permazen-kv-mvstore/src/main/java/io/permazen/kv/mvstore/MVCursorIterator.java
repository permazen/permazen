
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import org.h2.mvstore.Cursor;

/**
 * Converts a {@link Cursor} from an <code>Iterator&lt;ByteData&gt;</code> with inclusive upper bound
 * into an <code>Iterator&lt;KVPair&gt;</code> with exclusive upper bound.
 */
public class MVCursorIterator extends AbstractIterator<KVPair> {

    private final Cursor<ByteData, ByteData> cursor;
    private final ByteData maxKey;
    private final boolean reverse;

// Constructors

    /**
     * Constructor.
     *
     * @param cursor the underlying {@link Cursor}
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @param reverse true if {@code cursor} iterates keys in descending order
     * @throws IllegalArgumentException if {@code cursor} is null
     */
    public MVCursorIterator(Cursor<ByteData, ByteData> cursor, ByteData maxKey, boolean reverse) {
        Preconditions.checkArgument(cursor != null, "null cursor");
        this.cursor = cursor;
        this.maxKey = maxKey;
        this.reverse = reverse;
    }

    public ByteData getMaxKey() {
        return this.maxKey;
    }

    public boolean isReverse() {
        return this.reverse;
    }

// Iterator

    @Override
    protected KVPair computeNext() {
        while (true) {

            // Get the next key/value pair from underlying cursor
            if (!this.cursor.hasNext())
                return this.endOfData();
            final KVPair kv = new KVPair(this.cursor.next(), this.cursor.getValue());

            // We must manually exclude the "maxKey" exclusive bound
            if (this.maxKey != null && kv.getKey().compareTo(this.maxKey) >= 0) {
                if (!this.reverse)
                    return this.endOfData();                            // forward - this must be the last key returned
                continue;                                               // reverse - this must be the first key returned
            }

            // Done
            return kv;
        }
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[cursor=" + this.cursor
          + ",maxKey=" + ByteUtil.toString(this.maxKey)
          + ",reverse=" + this.reverse
          + "]";
    }
}
