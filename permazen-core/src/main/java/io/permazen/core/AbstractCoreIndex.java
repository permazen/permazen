
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.util.Bounds;

import java.util.Arrays;
import java.util.List;

/**
 * Support superclass for the various core index classes.
 */
abstract class AbstractCoreIndex {

    final KVStore kv;
    final AbstractIndexView indexView;

// Constructors

    protected AbstractCoreIndex(KVStore kv, int size, AbstractIndexView indexView) {
        Preconditions.checkArgument(kv != null, "null kv");
        Preconditions.checkArgument(indexView != null, "null indexView");
        this.kv = kv;
        this.indexView = indexView;
        if (this.indexView.encodings.length != size)
            throw new RuntimeException("internal error: indexView has the wrong size");
    }

// Methods

    /**
     * Get all of the {@link Encoding}s associated with this instance. The list includes an entry
     * for each indexed value type, followed by a final entry representing the index target type.
     *
     * @return unmodifiable list of encodings
     */
    public List<Encoding<?>> getEncodings() {
        return Arrays.asList(this.indexView.encodings.clone());
    }

    /**
     * Apply key filtering to field values at the specified index. This method works cummulatively: the new instance
     * filters to the intersection of the given key filter and any existing key filter on that field.
     *
     * @param index zero-based object type field offset
     * @param keyFilter key filtering to apply
     * @return filtered view of this instance
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     * @throws IllegalArgumentException if {@code keyFilter} is null
     */
    public abstract AbstractCoreIndex filter(int index, KeyFilter keyFilter);

    /**
     * Get a view of this index with the specified value restricted using the given bounds.
     *
     * @param index value's position in index
     * @param bounds bounds to impose on value
     * @return filtered view of this instance
     */
    <T> AbstractCoreIndex filter(int index, Encoding<T> encoding, Bounds<T> bounds) {
        assert encoding == this.indexView.encodings[index];
        final KeyRange range = encoding.getKeyRange(bounds);
        return !range.isFull() ? this.filter(index, new KeyRanges(range)) : this;
    }
}
