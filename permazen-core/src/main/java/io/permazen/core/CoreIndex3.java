
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Tuple4Encoding;
import io.permazen.index.Index1;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Core API {@link Index3} implementation representing a composite index on three fields.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V1> first index value type
 * @param <V2> second index value type
 * @param <V3> third index value type
 * @param <T> index target type
 */
public class CoreIndex3<V1, V2, V3, T> extends AbstractCoreIndex<T> implements Index3<V1, V2, V3, T> {

// Constructors

    CoreIndex3(KVStore kv, Index3View<V1, V2, V3, T> indexView) {
        super(kv, 4, indexView);
    }

// Methods

    @Override
    public CoreIndex3<V1, V2, V3, T> filter(int index, KeyFilter filter) {
        return new CoreIndex3<>(this.kv, this.getIndex3View().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    Index3View<V1, V2, V3, T> getIndex3View() {
        return (Index3View<V1, V2, V3, T>)this.indexView;
    }

// Index3

    @Override
    public NavigableSet<Tuple4<V1, V2, V3, T>> asSet() {

        // Get index view
        final Index3View<V1, V2, V3, T> iv = this.getIndex3View();

        // Create encoding for Tuple4<V1, V2, V3, T>
        final Tuple4Encoding<V1, V2, V3, T> encoding = new Tuple4Encoding<>(
          iv.getValue1Encoding(), iv.getValue2Encoding(), iv.getValue3Encoding(), iv.getTargetEncoding());

        // Build set and apply filtering
        IndexSet<Tuple4<V1, V2, V3, T>> indexSet = new IndexSet<>(this.kv,
          encoding, iv.prefixMode, iv.prefix);
        if (iv.hasFilters())
            indexSet = indexSet.filterKeys(new IndexKeyFilter(this.kv, iv, 4));

        // Done
        return indexSet;
    }

    @Override
    public NavigableMap<Tuple3<V1, V2, V3>, NavigableSet<T>> asMap() {

        // Get index view
        final Index3View<V1, V2, V3, T> iv = this.getIndex3View();

        // Create new IndexView
        final Index1View<Tuple3<V1, V2, V3>, T> tupleIV = iv.asTuple3Index1View();

        // Build map and apply filtering
        IndexMap<Tuple3<V1, V2, V3>, NavigableSet<T>> indexMap = new IndexMap.OfValues<>(this.kv, tupleIV);
        if (tupleIV.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<Tuple2<V1, V2>, Index1<V3, T>> asMapOfIndex1() {

        // Get index view
        final Index3View<V1, V2, V3, T> iv = this.getIndex3View();

        // Create new IndexView
        final Index2View<Tuple2<V1, V2>, V3, T> tupleIV = iv.asTuple2Index2View();

        // Build map and apply filtering
        IndexMap<Tuple2<V1, V2>, Index1<V3, T>> indexMap = new IndexMap.OfIndex1<>(this.kv, tupleIV);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<V1, Index2<V2, V3, T>> asMapOfIndex2() {

        // Get index view
        final Index3View<V1, V2, V3, T> iv = this.getIndex3View();

        // Build map and apply filtering
        IndexMap<V1, Index2<V2, V3, T>> indexMap = new IndexMap.OfIndex2<>(this.kv, iv);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    public CoreIndex2<V1, V2, V3> asIndex2() {
        return new CoreIndex2<>(this.kv, this.getIndex3View().asIndex2View());
    }

    @Override
    public CoreIndex1<V1, V2> asIndex1() {
        return new CoreIndex1<>(this.kv, this.getIndex3View().asIndex2View().asIndex1View());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex3<V1, V2, V3, T> withValue1Bounds(Bounds<V1> bounds) {
        return (CoreIndex3<V1, V2, V3, T>)this.filter(0, this.getIndex3View().getValue1Encoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex3<V1, V2, V3, T> withValue2Bounds(Bounds<V2> bounds) {
        return (CoreIndex3<V1, V2, V3, T>)this.filter(1, this.getIndex3View().getValue2Encoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex3<V1, V2, V3, T> withValue3Bounds(Bounds<V3> bounds) {
        return (CoreIndex3<V1, V2, V3, T>)this.filter(2, this.getIndex3View().getValue3Encoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex3<V1, V2, V3, T> withTargetBounds(Bounds<T> bounds) {
        return (CoreIndex3<V1, V2, V3, T>)this.filter(3, this.getIndex3View().getTargetEncoding(), bounds);
    }
}
