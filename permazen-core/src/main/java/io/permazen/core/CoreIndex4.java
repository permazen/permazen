
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.Tuple5Encoding;
import io.permazen.index.Index;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.index.Index4;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.tuple.Tuple5;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Core API {@link Index4} implementation representing a composite index on four fields.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V1> first index value type
 * @param <V2> second index value type
 * @param <V3> third index value type
 * @param <V4> fourth index value type
 * @param <T> index target type
 */
public class CoreIndex4<V1, V2, V3, V4, T> extends AbstractCoreIndex implements Index4<V1, V2, V3, V4, T> {

// Constructors

    CoreIndex4(KVStore kv, Index4View<V1, V2, V3, V4, T> indexView) {
        super(kv, 5, indexView);
    }

// Methods

    @Override
    public CoreIndex4<V1, V2, V3, V4, T> filter(int index, KeyFilter filter) {
        return new CoreIndex4<>(this.kv, this.getIndex4View().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    Index4View<V1, V2, V3, V4, T> getIndex4View() {
        return (Index4View<V1, V2, V3, V4, T>)this.indexView;
    }

// Index4

    @Override
    public NavigableSet<Tuple5<V1, V2, V3, V4, T>> asSet() {

        // Get index view
        final Index4View<V1, V2, V3, V4, T> iv = this.getIndex4View();

        // Create encoding for Tuple5<V1, V2, V3, V4, T>
        final Tuple5Encoding<V1, V2, V3, V4, T> encoding = new Tuple5Encoding<>(
          iv.getValue1Type(), iv.getValue2Type(), iv.getValue3Type(), iv.getValue4Type(), iv.getTargetType());

        // Build set and apply filtering
        IndexSet<Tuple5<V1, V2, V3, V4, T>> indexSet = new IndexSet<>(this.kv,
          encoding, iv.prefixMode, iv.prefix);
        if (iv.hasFilters())
            indexSet = indexSet.filterKeys(new IndexKeyFilter(this.kv, iv, 5));

        // Done
        return indexSet;
    }

    @Override
    public NavigableMap<Tuple4<V1, V2, V3, V4>, NavigableSet<T>> asMap() {

        // Get index view
        final Index4View<V1, V2, V3, V4, T> iv = this.getIndex4View();

        // Create new IndexView
        final IndexView<Tuple4<V1, V2, V3, V4>, T> tupleIV = iv.asTuple4IndexView();

        // Build map and apply filtering
        IndexMap<Tuple4<V1, V2, V3, V4>, NavigableSet<T>> indexMap = new IndexMap.OfValues<>(
          this.kv, tupleIV);
        if (tupleIV.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<Tuple3<V1, V2, V3>, Index<V4, T>> asMapOfIndex() {

        // Get index view
        final Index4View<V1, V2, V3, V4, T> iv = this.getIndex4View();

        // Create new IndexView
        final Index2View<Tuple3<V1, V2, V3>, V4, T> tupleIV = iv.asTuple3Index2View();

        // Build map and apply filtering
        IndexMap<Tuple3<V1, V2, V3>, Index<V4, T>> indexMap = new IndexMap.OfIndex<>(this.kv, tupleIV);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<Tuple2<V1, V2>, Index2<V3, V4, T>> asMapOfIndex2() {

        // Get index view
        final Index4View<V1, V2, V3, V4, T> iv = this.getIndex4View();

        // Create new IndexView
        final Index3View<Tuple2<V1, V2>, V3, V4, T> tupleIV = iv.asTuple2Index3View();

        // Build map and apply filtering
        IndexMap<Tuple2<V1, V2>, Index2<V3, V4, T>> indexMap = new IndexMap.OfIndex2<>(this.kv, tupleIV);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public NavigableMap<V1, Index3<V2, V3, V4, T>> asMapOfIndex3() {

        // Get index view
        final Index4View<V1, V2, V3, V4, T> iv = this.getIndex4View();

        // Build map and apply filtering
        IndexMap<V1, Index3<V2, V3, V4, T>> indexMap = new IndexMap.OfIndex3<>(this.kv, iv);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    public CoreIndex3<V1, V2, V3, V4> asIndex3() {
        return new CoreIndex3<>(this.kv, this.getIndex4View().asIndex3View());
    }

    @Override
    public CoreIndex2<V1, V2, V3> asIndex2() {
        return new CoreIndex2<>(this.kv, this.getIndex4View().asIndex3View().asIndex2View());
    }

    @Override
    public CoreIndex<V1, V2> asIndex() {
        return new CoreIndex<>(this.kv, this.getIndex4View().asIndex3View().asIndex2View().asIndexView());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex4<V1, V2, V3, V4, T> withValue1Bounds(Bounds<V1> bounds) {
        return (CoreIndex4<V1, V2, V3, V4, T>)this.filter(0, this.getIndex4View().getValue1Type(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex4<V1, V2, V3, V4, T> withValue2Bounds(Bounds<V2> bounds) {
        return (CoreIndex4<V1, V2, V3, V4, T>)this.filter(1, this.getIndex4View().getValue2Type(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex4<V1, V2, V3, V4, T> withValue3Bounds(Bounds<V3> bounds) {
        return (CoreIndex4<V1, V2, V3, V4, T>)this.filter(2, this.getIndex4View().getValue3Type(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex4<V1, V2, V3, V4, T> withValue4Bounds(Bounds<V4> bounds) {
        return (CoreIndex4<V1, V2, V3, V4, T>)this.filter(3, this.getIndex4View().getValue4Type(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex4<V1, V2, V3, V4, T> withTargetBounds(Bounds<T> bounds) {
        return (CoreIndex4<V1, V2, V3, V4, T>)this.filter(4, this.getIndex4View().getTargetType(), bounds);
    }
}
