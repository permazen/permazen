
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Tuple3Encoding;
import io.permazen.index.Index;
import io.permazen.index.Index2;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Core API {@link Index2} implementation representing a composite index on two fields.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V1> first index value type
 * @param <V2> second index value type
 * @param <T> index target type
 */
public class CoreIndex2<V1, V2, T> extends AbstractCoreIndex implements Index2<V1, V2, T> {

// Constructors

    CoreIndex2(KVStore kv, Index2View<V1, V2, T> indexView) {
        super(kv, 3, indexView);
    }

// Methods

    @Override
    public CoreIndex2<V1, V2, T> filter(int index, KeyFilter filter) {
        return new CoreIndex2<>(this.kv, this.getIndex2View().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    Index2View<V1, V2, T> getIndex2View() {
        return (Index2View<V1, V2, T>)this.indexView;
    }

// Index2

    @Override
    public NavigableSet<Tuple3<V1, V2, T>> asSet() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Create encoding for Tuple3<V1, V2, T>
        final Tuple3Encoding<V1, V2, T> encoding = new Tuple3Encoding<>(
          iv.getValue1Encoding(), iv.getValue2Encoding(), iv.getTargetEncoding());

        // Build set and apply filtering
        IndexSet<Tuple3<V1, V2, T>> indexSet = new IndexSet<>(this.kv, encoding, iv.prefixMode, iv.prefix);
        if (iv.hasFilters())
            indexSet = indexSet.filterKeys(new IndexKeyFilter(this.kv, iv, 3));

        // Done
        return indexSet;
    }

    @Override
    public NavigableMap<Tuple2<V1, V2>, NavigableSet<T>> asMap() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Create new IndexView
        final IndexView<Tuple2<V1, V2>, T> tupleIV = iv.asTuple2IndexView();

        // Build map and apply filtering
        IndexMap<Tuple2<V1, V2>, NavigableSet<T>> indexMap = new IndexMap.OfValues<>(this.kv, tupleIV);
        if (tupleIV.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<V1, Index<V2, T>> asMapOfIndex() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Build map and apply filtering
        IndexMap<V1, Index<V2, T>> indexMap = new IndexMap.OfIndex<>(this.kv, iv);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    public CoreIndex<V1, V2> asIndex() {
        return new CoreIndex<>(this.kv, this.getIndex2View().asIndexView());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex2<V1, V2, T> withValue1Bounds(Bounds<V1> bounds) {
        return (CoreIndex2<V1, V2, T>)this.filter(0, this.getIndex2View().getValue1Encoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex2<V1, V2, T> withValue2Bounds(Bounds<V2> bounds) {
        return (CoreIndex2<V1, V2, T>)this.filter(1, this.getIndex2View().getValue2Encoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex2<V1, V2, T> withTargetBounds(Bounds<T> bounds) {
        return (CoreIndex2<V1, V2, T>)this.filter(2, this.getIndex2View().getTargetEncoding(), bounds);
    }
}
