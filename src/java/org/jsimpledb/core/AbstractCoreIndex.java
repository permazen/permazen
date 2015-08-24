
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

import org.jsimpledb.kv.KeyFilter;

/**
 * Support superclass for the various core index classes.
 */
abstract class AbstractCoreIndex {

    final Transaction tx;
    final AbstractIndexView indexView;

// Constructors

    protected AbstractCoreIndex(Transaction tx, int size, AbstractIndexView indexView) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(indexView != null, "null indexView");
        this.tx = tx;
        this.indexView = indexView;
        if (this.indexView.fieldTypes.length != size)
            throw new RuntimeException("internal error: indexView has the wrong size");
    }

// Methods

    /**
     * Get all of the {@link FieldType}s associated with this instance. The list includes an entry
     * for each indexed value type, followed by a final entry representing the index target type.
     *
     * @return unmodifiable list of field types
     */
    public List<FieldType<?>> getFieldTypes() {
        return Arrays.asList(this.indexView.fieldTypes.clone());
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
}

