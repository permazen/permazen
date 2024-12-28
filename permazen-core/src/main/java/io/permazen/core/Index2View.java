
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.encoding.Tuple2Encoding;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A view of an index on two values.
 */
class Index2View<V1, V2, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param value1Encoding first value encoding
     * @param value2Encoding second value encoding
     * @param targetEncoding index target encoding
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index2View(int storageId, Encoding<V1> value1Encoding, Encoding<V2> value2Encoding, Encoding<T> targetEncoding) {
        this(UnsignedIntEncoder.encode(storageId), false, value1Encoding, value2Encoding, targetEncoding);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetEncoding} is not the final field in the index
     * @param value1Encoding first value encoding
     * @param value2Encoding second value encoding
     * @param targetEncoding index target encoding
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index2View(ByteData prefix, boolean prefixMode,
      Encoding<V1> value1Encoding, Encoding<V2> value2Encoding, Encoding<T> targetEncoding) {
        super(prefix, prefixMode, value1Encoding, value2Encoding, targetEncoding);
    }

    // Internal copy constructor
    private Index2View(Index2View<V1, V2, T> original) {
        super(original);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V1> getValue1Encoding() {
        return (Encoding<V1>)this.encodings[0];
    }

    @SuppressWarnings("unchecked")
    public Encoding<V2> getValue2Encoding() {
        return (Encoding<V2>)this.encodings[1];
    }

    @SuppressWarnings("unchecked")
    public Encoding<T> getTargetEncoding() {
        return (Encoding<T>)this.encodings[2];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index2View<V1, V2, T> filter(int index, KeyFilter keyFilter) {
        return (Index2View<V1, V2, T>)super.filter(index, keyFilter);
    }

    @Override
    protected Index2View<V1, V2, T> copy() {
        return new Index2View<>(this);
    }

// Tuple views

    public Index1View<Tuple2<V1, V2>, T> asTuple2Index1View() {

        // Create new IndexView
        Index1View<Tuple2<V1, V2>, T> indexView = new Index1View<>(this.prefix, this.prefixMode,
          new Tuple2Encoding<>(this.getValue1Encoding(), this.getValue2Encoding()), this.getTargetEncoding());

        // Apply filters
        final KeyFilter value1Filter = this.getFilter(0);
        final KeyFilter value2Filter = this.getFilter(1);
        final KeyFilter targetFilter = this.getFilter(2);

        // Apply filtering to tuple field
        if (value1Filter != null || value2Filter != null) {
            EncodingsFilter tupleFilter = new EncodingsFilter(null, this.getValue1Encoding(), this.getValue2Encoding());
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

    public Index1View<V1, V2> asIndex1View() {

        // Create IndexView
        Index1View<V1, V2> indexView = new Index1View<>(this.prefix, true, this.getValue1Encoding(), this.getValue2Encoding());

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

    public Index1View<V2, T> asIndex1View(ByteData keyPrefix) {

        // Create IndexView
        Index1View<V2, T> indexView = new Index1View<>(keyPrefix,
          this.prefixMode, this.getValue2Encoding(), this.getTargetEncoding());

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
