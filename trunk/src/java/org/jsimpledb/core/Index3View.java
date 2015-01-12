
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A view of an index on three values.
 */
class Index3View<V1, V2, V3, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param value1Type first value type
     * @param value2Type second value type
     * @param value3Type third value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    public Index3View(int storageId, FieldType<V1> value1Type,
      FieldType<V2> value2Type, FieldType<V3> value3Type, FieldType<T> targetType) {
        this(UnsignedIntEncoder.encode(storageId), false, value1Type, value2Type, value3Type, targetType);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetType} is not the final field in the index
     * @param value1Type first value type
     * @param value2Type second value type
     * @param value3Type third value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    public Index3View(byte[] prefix, boolean prefixMode,
      FieldType<V1> value1Type, FieldType<V2> value2Type, FieldType<V3> value3Type, FieldType<T> targetType) {
        super(prefix, prefixMode, value1Type, value2Type, value3Type, targetType);
    }

    // Internal copy constructor
    private Index3View(Index3View<V1, V2, V3, T> original) {
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
    public FieldType<V3> getValue3Type() {
        return (FieldType<V3>)this.fieldTypes[2];
    }

    @SuppressWarnings("unchecked")
    public FieldType<T> getTargetType() {
        return (FieldType<T>)this.fieldTypes[3];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index3View<V1, V2, V3, T> filter(int index, KeyFilter keyFilter) {
        return (Index3View<V1, V2, V3, T>)super.filter(index, keyFilter);
    }

    @Override
    protected Index3View<V1, V2, V3, T> copy() {
        return new Index3View<V1, V2, V3, T>(this);
    }

// Tuple views

    public IndexView<Tuple3<V1, V2, V3>, T> asTuple3IndexView() {

        // Create new IndexView
        IndexView<Tuple3<V1, V2, V3>, T> indexView = new IndexView<Tuple3<V1, V2, V3>, T>(this.prefix, this.prefixMode,
          new Tuple3FieldType<V1, V2, V3>(this.getValue1Type(), this.getValue2Type(), this.getValue3Type()), this.getTargetType());

        // Get filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter targetFilter = this.getFilter(3);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null || value3Filter != null) {
            FieldTypesFilter tupleFilter = new FieldTypesFilter(null,
              this.getValue1Type(), this.getValue2Type(), this.getValue3Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            if (value3Filter != null)
                tupleFilter = tupleFilter.filter(2, value3Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(1, targetFilter);

        // Done
        return indexView;
    }

    public Index2View<Tuple2<V1, V2>, V3, T> asTuple2Index2View() {

        // Create new IndexView
        Index2View<Tuple2<V1, V2>, V3, T> indexView = new Index2View<Tuple2<V1, V2>, V3, T>(this.prefix, this.prefixMode,
          new Tuple2FieldType<V1, V2>(this.getValue1Type(), this.getValue2Type()), this.getValue3Type(), this.getTargetType());

        // Apply filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter targetFilter = this.getFilter(3);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null) {
            FieldTypesFilter tupleFilter = new FieldTypesFilter(null, this.getValue1Type(), this.getValue2Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to value field #3
        if (value3Filter != null)
            indexView = indexView.filter(1, value3Filter);

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(2, targetFilter);

        // Done
        return indexView;
    }

// Prefix view

    public Index2View<V1, V2, V3> asIndex2View() {

        // Create IndexView
        Index2View<V1, V2, V3> indexView = new Index2View<V1, V2, V3>(this.prefix,
          true, this.getValue1Type(), this.getValue2Type(), this.getValue3Type());

        // Apply filters
        final KeyFilter value1Filter = this.getFilter(0);
        if (value1Filter != null)
            indexView = indexView.filter(0, value1Filter);
        final KeyFilter value2Filter = this.getFilter(1);
        if (value2Filter != null)
            indexView = indexView.filter(1, value2Filter);
        final KeyFilter value3Filter = this.getFilter(2);
        if (value3Filter != null)
            indexView = indexView.filter(2, value3Filter);

        // Done
        return indexView;
    }

// Suffix view

    public Index2View<V2, V3, T> asIndex2View(byte[] keyPrefix) {

        // Create IndexView
        Index2View<V2, V3, T> indexView = new Index2View<V2, V3, T>(keyPrefix,
          this.prefixMode, this.getValue2Type(), this.getValue3Type(), this.getTargetType());

        // Apply filters
        final KeyFilter value2Filter = this.getFilter(1);
        if (value2Filter != null)
            indexView = indexView.filter(0, value2Filter);
        final KeyFilter value3Filter = this.getFilter(2);
        if (value3Filter != null)
            indexView = indexView.filter(1, value3Filter);
        final KeyFilter targetFilter = this.getFilter(3);
        if (targetFilter != null)
            indexView = indexView.filter(2, targetFilter);

        // Done
        return indexView;
    }
}

