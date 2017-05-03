
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.core.type.Tuple2FieldType;
import org.jsimpledb.index.Index;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.tuple.Tuple2;

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

        // Create tuple field type
        final Tuple2FieldType<V, T> tupleFieldType = new Tuple2FieldType<>(iv.getValueType(), iv.getTargetType());

        // Build set and apply filtering
        IndexSet<Tuple2<V, T>> indexSet = new IndexSet<>(this.kv, tupleFieldType, iv.prefixMode, iv.prefix);
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
}

