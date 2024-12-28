
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.NavigableSet;

/**
 * Implements the {@link NavigableSet} view of an index.
 *
 * @param <E> type of the values being indexed
 */
class IndexSet<E> extends EncodingSet<E> {

    // Primary constructor
    IndexSet(KVStore kv, Encoding<E> entryType, boolean prefixMode, ByteData prefix) {
        super(kv, entryType, prefixMode, prefix);
    }

    // Internal constructor
    private IndexSet(KVStore kv, Encoding<E> entryType, boolean prefixMode, boolean reversed,
      ByteData prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(kv, entryType, prefixMode, reversed, prefix, keyRange, keyFilter, bounds);
    }

    public String getDescription() {
        return "IndexSet"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",encoding=" + this.encoding
          + (this.bounds != null ? ",bounds=" + this.bounds : "")
          + (this.keyRange != null ? ",keyRange=" + this.keyRange : "")
          + (this.keyFilter != null ? ",keyFilter=" + this.keyFilter : "")
          + (this.reversed ? ",reversed" : "")
          + "]";
    }

// AbstractKVNavigableSet

    @Override
    protected NavigableSet<E> createSubSet(boolean newReversed, KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds) {
        return new IndexSet<>(this.kv, this.encoding,
          this.prefixMode, newReversed, this.prefix, newKeyRange, newKeyFilter, newBounds);
    }

    @Override
    public IndexSet<E> filterKeys(KeyFilter keyFilter) {
        return (IndexSet<E>)super.filterKeys(keyFilter);
    }
}
