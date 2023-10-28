
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.Tuple2Encoding;
import io.permazen.index.Index;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.tuple.Tuple2;
import io.permazen.util.Bounds;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Core API {@link Index} implementation representing a index on a single field.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <V> index value type
 * @param <T> index target type
 */
public class CoreIndex<V, T> extends AbstractCoreIndex implements Index<V, T> {

// Constructors

    CoreIndex(KVStore kv, IndexView<V, T> indexView) {
        super(kv, 2, indexView);
    }

// Methods

    @Override
    public CoreIndex<V, T> filter(int index, KeyFilter filter) {
        return new CoreIndex<>(this.kv, this.getIndexView().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    IndexView<V, T> getIndexView() {
        return (IndexView<V, T>)this.indexView;
    }

// Index

    @Override
    public NavigableSet<Tuple2<V, T>> asSet() {

        // Get index view
        final IndexView<V, T> iv = this.getIndexView();

        // Create tuple encoding
        final Tuple2Encoding<V, T> tupleEncoding = new Tuple2Encoding<>(iv.getValueEncoding(), iv.getTargetEncoding());

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
        final IndexView<V, T> iv = this.getIndexView();

        // Build map and apply filtering
        IndexMap<V, NavigableSet<T>> indexMap = new IndexMap.OfValues<>(this.kv, iv);
        if (this.indexView.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.kv, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex<V, T> withValueBounds(Bounds<V> bounds) {
        return (CoreIndex<V, T>)this.filter(0, this.getIndexView().getValueEncoding(), bounds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreIndex<V, T> withTargetBounds(Bounds<T> bounds) {
        return (CoreIndex<V, T>)this.filter(1, this.getIndexView().getTargetEncoding(), bounds);
    }
}
