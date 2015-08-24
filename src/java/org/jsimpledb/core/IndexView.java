
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A view of an index on a single value.
 */
class IndexView<V, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param valueType index value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    public IndexView(int storageId, FieldType<V> valueType, FieldType<T> targetType) {
        this(UnsignedIntEncoder.encode(storageId), false, valueType, targetType);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetType} is not the final field in the index
     * @param valueType index value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    public IndexView(byte[] prefix, boolean prefixMode, FieldType<V> valueType, FieldType<T> targetType) {
        super(prefix, prefixMode, valueType, targetType);
    }

    // Internal copy constructor
    private IndexView(IndexView<V, T> original) {
        super(original);
    }

    @SuppressWarnings("unchecked")
    public FieldType<V> getValueType() {
        return (FieldType<V>)this.fieldTypes[0];
    }

    @SuppressWarnings("unchecked")
    public FieldType<T> getTargetType() {
        return (FieldType<T>)this.fieldTypes[1];
    }

    @Override
    @SuppressWarnings("unchecked")
    public IndexView<V, T> filter(int index, KeyFilter keyFilter) {
        return (IndexView<V, T>)super.filter(index, keyFilter);
    }

    @Override
    protected IndexView<V, T> copy() {
        return new IndexView<V, T>(this);
    }
}

