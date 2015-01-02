
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteUtil;

/**
 * Implements the {@link NavigableSet} view of an index.
 *
 * @param <E> type of the values being indexed
 */
class IndexSet<E> extends FieldTypeSet<E> {

    // Primary constructor
    IndexSet(Transaction tx, FieldType<E> entryType, boolean prefixMode, byte[] prefix) {
        super(tx, entryType, prefixMode, prefix);
    }

    // Internal constructor
    private IndexSet(Transaction tx, FieldType<E> entryType, boolean prefixMode, boolean reversed,
      byte[] prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(tx, entryType, prefixMode, reversed, prefix, keyRange, keyFilter, bounds);
    }

    public String getDescription() {
        return "IndexSet"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",fieldType=" + this.fieldType
          + (this.bounds != null ? ",bounds=" + this.bounds : "")
          + (this.keyRange != null ? ",keyRange=" + this.keyRange : "")
          + (this.keyFilter != null ? ",keyFilter=" + this.keyFilter : "")
          + (this.reversed ? ",reversed" : "")
          + "]";
    }

// AbstractKVNavigableSet

    @Override
    protected NavigableSet<E> createSubSet(boolean newReversed, KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds) {
        return new IndexSet<E>(this.tx, this.fieldType,
          this.prefixMode, newReversed, this.prefix, newKeyRange, newKeyFilter, newBounds);
    }

    @Override
    public IndexSet<E> filterKeys(KeyFilter keyFilter) {
        return (IndexSet<E>)super.filterKeys(keyFilter);
    }
}

