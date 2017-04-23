
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;

/**
 * Core API {@link Index} implementation representing a composite index on two fields.
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

        // Create field type for Tuple3<V1, V2, T>
        final Tuple3FieldType<V1, V2, T> fieldType = new Tuple3FieldType<>(
          iv.getValue1Type(), iv.getValue2Type(), iv.getTargetType());

        // Build set and apply filtering
        IndexSet<Tuple3<V1, V2, T>> indexSet = new IndexSet<>(this.kv, fieldType, iv.prefixMode, iv.prefix);
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
}

