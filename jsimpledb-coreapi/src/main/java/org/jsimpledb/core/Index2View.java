
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A view of an index on two values.
 */
class Index2View<V1, V2, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param value1Type first value type
     * @param value2Type second value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index2View(int storageId, FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<T> targetType) {
        this(UnsignedIntEncoder.encode(storageId), false, value1Type, value2Type, targetType);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetType} is not the final field in the index
     * @param value1Type first value type
     * @param value2Type second value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index2View(byte[] prefix, boolean prefixMode,
      FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<T> targetType) {
        super(prefix, prefixMode, value1Type, value2Type, targetType);
    }

    // Internal copy constructor
    private Index2View(Index2View<V1, V2, T> original) {
        super(original);
    }

    @SuppressWarnings("unchecked")
    public FieldType<V1> getValue1Type() {
        return (FieldType<V1>)this.fieldTypes[0];
    }

    @SuppressWarnings("unchecked")
    public FieldType<V2> getValue2Type() {
        return (FieldType<V2>)this.fieldTypes[1];
    }

    @SuppressWarnings("unchecked")
    public FieldType<T> getTargetType() {
        return (FieldType<T>)this.fieldTypes[2];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index2View<V1, V2, T> filter(int index, KeyFilter keyFilter) {
        return (Index2View<V1, V2, T>)super.filter(index, keyFilter);
    }

    @Override
    protected Index2View<V1, V2, T> copy() {
        return new Index2View<V1, V2, T>(this);
    }

// Tuple views

    public IndexView<Tuple2<V1, V2>, T> asTuple2IndexView() {

        // Create new IndexView
        IndexView<Tuple2<V1, V2>, T> indexView = new IndexView<Tuple2<V1, V2>, T>(this.prefix, this.prefixMode,
          new Tuple2FieldType<V1, V2>(this.getValue1Type(), this.getValue2Type()), this.getTargetType());

        // Apply filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter targetFilter = this.getFilter(2);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null) {
            FieldTypesFilter tupleFilter = new FieldTypesFilter(null, this.getValue1Type(), this.getValue2Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(1, targetFilter);

        // Done
        return indexView;
    }

// Prefix view

    public IndexView<V1, V2> asIndexView() {

        // Create IndexView
        IndexView<V1, V2> indexView = new IndexView<V1, V2>(this.prefix, true, this.getValue1Type(), this.getValue2Type());

        // Apply filters
        final KeyFilter value1Filter = this.getFilter(0);
        if (value1Filter != null)
            indexView = indexView.filter(0, value1Filter);
        final KeyFilter value2Filter = this.getFilter(1);
        if (value2Filter != null)
            indexView = indexView.filter(1, value2Filter);

        // Done
        return indexView;
    }

// Suffix view

    public IndexView<V2, T> asIndexView(byte[] keyPrefix) {

        // Create IndexView
        IndexView<V2, T> indexView = new IndexView<V2, T>(keyPrefix, this.prefixMode, this.getValue2Type(), this.getTargetType());

        // Apply filters
        final KeyFilter value2Filter = this.getFilter(1);
        if (value2Filter != null)
            indexView = indexView.filter(0, value2Filter);
        final KeyFilter targetFilter = this.getFilter(2);
        if (targetFilter != null)
            indexView = indexView.filter(1, targetFilter);

        // Done
        return indexView;
    }
}

