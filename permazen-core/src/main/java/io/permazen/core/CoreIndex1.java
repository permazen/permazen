
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Tuple2Encoding;
import io.permazen.index.Index1;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Core API {@link Index1} implementation representing an index on a single field.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V> index value type
 * @param <T> index target type
 */
public class CoreIndex1<V, T> extends AbstractCoreIndex<T> implements Index1<V, T> {

// Constructors

    CoreIndex1(KVStore kv, Index1View<V, T> indexView) {
        super(kv, 2, indexView);
    }

// Methods

    @Override
    public CoreIndex1<V, T> filter(int index, KeyFilter filter) {
        return new CoreIndex1<>(this.kv, this.getIndex1View().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    Index1View<V, T> getIndex1View() {
        return (Index1View<V, T>)this.indexView;
    }

// Index1

    @Override
    public NavigableSet<Tuple2<V, T>> asSet() {

        // Get index view
        final Index1View<V, T> iv = this.getIndex1View();

        // Create tuple encoding
        final Tuple2Encoding<V, T> tupleEncoding = new Tuple2Encoding<>(null, iv.getValueEncoding(), iv.getTargetEncoding());

        // Build set and apply filtering
        IndexSet<Tuple2<V, T>> indexSet = new IndexSet<>(this.kv, tupleEncoding, iv.prefixMode, iv.prefix);
        if (iv.hasFilters())
            indexSet = indexSet.filterKeys(new IndexKeyFilter(this.kv, iv, 2));

        // Done
        return indexSet;
    }

    @Override
    public NavigableMap<V, NavigableSet<T>> asMap() {

        // Get index view
        final Index1View<V, T> iv = this.getIndex1View();

        // Build map and apply filtering
        IndexMap<V, NavigableSet<T>> indexMap = new IndexMap.OfValues<>(this.kv, iv);
        if (this.indexView.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex1<V, T> withValueBounds(Bounds<V> bounds) {
        return (CoreIndex1<V, T>)this.filter(0, this.getIndex1View().getValueEncoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex1<V, T> withTargetBounds(Bounds<T> bounds) {
        return (CoreIndex1<V, T>)this.filter(1, this.getIndex1View().getTargetEncoding(), bounds);
    }
}
