
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.type.Tuple2Encoding;
import io.permazen.core.type.Tuple3Encoding;
import io.permazen.core.type.Tuple4Encoding;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A view of an index on four values.
 */
class Index4View<V1, V2, V3, V4, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param value1Type first value type
     * @param value2Type second value type
     * @param value3Type third value type
     * @param value4Type fourth value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index4View(int storageId, Encoding<V1> value1Type,
      Encoding<V2> value2Type, Encoding<V3> value3Type, Encoding<V4> value4Type, Encoding<T> targetType) {
        this(UnsignedIntEncoder.encode(storageId), false, value1Type, value2Type, value3Type, value4Type, targetType);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetType} is not the final field in the index
     * @param value1Type first value type
     * @param value2Type second value type
     * @param value3Type third value type
     * @param value4Type fourth value type
     * @param targetType index target type
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index4View(byte[] prefix, boolean prefixMode, Encoding<V1> value1Type,
      Encoding<V2> value2Type, Encoding<V3> value3Type, Encoding<V4> value4Type, Encoding<T> targetType) {
        super(prefix, prefixMode, value1Type, value2Type, value3Type, value4Type, targetType);
    }

    // Internal copy constructor
    private Index4View(Index4View<V1, V2, V3, V4, T> original) {
        super(original);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V1> getValue1Type() {
        return (Encoding<V1>)this.encodings[0];
    }

    @SuppressWarnings("unchecked")
    public Encoding<V2> getValue2Type() {
        return (Encoding<V2>)this.encodings[1];
    }

    @SuppressWarnings("unchecked")
    public Encoding<V3> getValue3Type() {
        return (Encoding<V3>)this.encodings[2];
    }

    @SuppressWarnings("unchecked")
    public Encoding<V4> getValue4Type() {
        return (Encoding<V4>)this.encodings[3];
    }

    @SuppressWarnings("unchecked")
    public Encoding<T> getTargetType() {
        return (Encoding<T>)this.encodings[4];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index4View<V1, V2, V3, V4, T> filter(int index, KeyFilter keyFilter) {
        return (Index4View<V1, V2, V3, V4, T>)super.filter(index, keyFilter);
    }

    @Override
    protected Index4View<V1, V2, V3, V4, T> copy() {
        return new Index4View<>(this);
    }

// Tuple views

    public IndexView<Tuple4<V1, V2, V3, V4>, T> asTuple4IndexView() {

        // Create new IndexView
        IndexView<Tuple4<V1, V2, V3, V4>, T> indexView = new IndexView<>(this.prefix, this.prefixMode,
          new Tuple4Encoding<>(this.getValue1Type(), this.getValue2Type(),
            this.getValue3Type(), this.getValue4Type()), this.getTargetType());

        // Get filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter value4Filter = this.getFilter(3);
        final KeyFilter targetFilter = this.getFilter(4);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null || value3Filter != null || value4Filter != null) {
            EncodingsFilter tupleFilter = new EncodingsFilter(null,
              this.getValue1Type(), this.getValue2Type(), this.getValue3Type(), this.getValue4Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            if (value3Filter != null)
                tupleFilter = tupleFilter.filter(2, value3Filter);
            if (value4Filter != null)
                tupleFilter = tupleFilter.filter(3, value4Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(1, targetFilter);

        // Done
        return indexView;
    }

    public Index2View<Tuple3<V1, V2, V3>, V4, T> asTuple3Index2View() {

        // Create new IndexView
        Index2View<Tuple3<V1, V2, V3>, V4, T> indexView = new Index2View<>(this.prefix, this.prefixMode,
          new Tuple3Encoding<>(this.getValue1Type(), this.getValue2Type(), this.getValue3Type()), this.getValue4Type(),
          this.getTargetType());

        // Get filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter value4Filter = this.getFilter(3);
        final KeyFilter targetFilter = this.getFilter(4);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null || value3Filter != null) {
            EncodingsFilter tupleFilter = new EncodingsFilter(null,
              this.getValue1Type(), this.getValue2Type(), this.getValue3Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            if (value3Filter != null)
                tupleFilter = tupleFilter.filter(2, value3Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to value field #4
        if (value4Filter != null)
            indexView = indexView.filter(1, value4Filter);

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(2, targetFilter);

        // Done
        return indexView;
    }

    public Index3View<Tuple2<V1, V2>, V3, V4, T> asTuple2Index3View() {

        // Create new IndexView
        Index3View<Tuple2<V1, V2>, V3, V4, T> indexView = new Index3View<>(this.prefix, this.prefixMode,
          new Tuple2Encoding<>(this.getValue1Type(), this.getValue2Type()), this.getValue3Type(), this.getValue4Type(),
          this.getTargetType());

        // Get filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter value4Filter = this.getFilter(3);
        final KeyFilter targetFilter = this.getFilter(4);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null) {
            EncodingsFilter tupleFilter = new EncodingsFilter(null, this.getValue1Type(), this.getValue2Type());
            if (value1Filter != null)
                tupleFilter = tupleFilter.filter(0, value1Filter);
            if (value2Filter != null)
                tupleFilter = tupleFilter.filter(1, value2Filter);
            indexView = indexView.filter(0, tupleFilter);
        }

        // Apply filtering to value field #3
        if (value3Filter != null)
            indexView = indexView.filter(1, value3Filter);

        // Apply filtering to value field #4
        if (value4Filter != null)
            indexView = indexView.filter(2, value4Filter);

        // Apply filtering to target field
        if (targetFilter != null)
            indexView = indexView.filter(3, targetFilter);

        // Done
        return indexView;
    }

// Prefix view

    public Index3View<V1, V2, V3, V4> asIndex3View() {

        // Create IndexView
        Index3View<V1, V2, V3, V4> indexView = new Index3View<>(this.prefix,
          true, this.getValue1Type(), this.getValue2Type(), this.getValue3Type(), this.getValue4Type());

        // Get filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter value4Filter = this.getFilter(3);

        // Apply filters
        if (value1Filter != null)
            indexView = indexView.filter(0, value1Filter);
        if (value2Filter != null)
            indexView = indexView.filter(1, value2Filter);
        if (value3Filter != null)
            indexView = indexView.filter(2, value3Filter);
        if (value4Filter != null)
            indexView = indexView.filter(3, value4Filter);

        // Done
        return indexView;
    }

// Suffix view

    public Index3View<V2, V3, V4, T> asIndex3View(byte[] keyPrefix) {

        // Create IndexView
        Index3View<V2, V3, V4, T> indexView = new Index3View<>(keyPrefix,
          this.prefixMode, this.getValue2Type(), this.getValue3Type(), this.getValue4Type(), this.getTargetType());

        // Get filters
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter value3Filter = this.getFilter(2);
        final KeyFilter value4Filter = this.getFilter(3);
        final KeyFilter targetFilter = this.getFilter(4);

        // Apply filters
        if (value2Filter != null)
            indexView = indexView.filter(0, value2Filter);
        if (value3Filter != null)
            indexView = indexView.filter(1, value3Filter);
        if (value4Filter != null)
            indexView = indexView.filter(2, value4Filter);
        if (targetFilter != null)
            indexView = indexView.filter(3, targetFilter);

        // Done
        return indexView;
    }
}
