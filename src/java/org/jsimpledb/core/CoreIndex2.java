
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;

/**
 * Core API {@link Index} implementation representing a composite index on two fields.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @param <V1> first index value type
 * @param <V2> second index value type
 * @param <T> index target type
 */
public class CoreIndex2<V1, V2, T> extends AbstractCoreIndex implements Index2<V1, V2, T> {

// Constructors

    CoreIndex2(Transaction tx, Index2View<V1, V2, T> indexView) {
        super(tx, 3, indexView);
    }

// Methods

    @Override
    public CoreIndex2<V1, V2, T> filter(int index, KeyFilter filter) {
        return new CoreIndex2<V1, V2, T>(this.tx, this.getIndex2View().filter(index, filter));
    }

    @SuppressWarnings("unchecked")
    Index2View<V1, V2, T> getIndex2View() {
        return (Index2View<V1, V2, T>)this.indexView;
    }

// Index2

    @Override
    public IndexSet<Tuple3<V1, V2, T>> asSet() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Create field type for Tuple3<V1, V2, T>
        final Tuple3FieldType<V1, V2, T> fieldType = new Tuple3FieldType<V1, V2, T>(
          iv.getValue1Type(), iv.getValue2Type(), iv.getTargetType());

        // Build set and apply filtering
        IndexSet<Tuple3<V1, V2, T>> indexSet = new IndexSet<Tuple3<V1, V2, T>>(this.tx, fieldType, iv.prefixMode, iv.prefix);
        if (iv.hasFilters())
            indexSet = indexSet.filterKeys(new IndexKeyFilter(this.tx, iv, 3));

        // Done
        return indexSet;
    }

    @Override
    public IndexMap<Tuple2<V1, V2>, NavigableSet<T>> asMap() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Create new IndexView
        final IndexView<Tuple2<V1, V2>, T> tupleIV = iv.asTuple2IndexView();

        // Build map and apply filtering
        IndexMap<Tuple2<V1, V2>, NavigableSet<T>> indexMap = new IndexMap.OfValues<Tuple2<V1, V2>, T>(this.tx, tupleIV);
        if (tupleIV.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.tx, tupleIV, 1));

        // Done
        return indexMap;
    }

    @Override
    public IndexMap<V1, Index<V2, T>> asMapOfIndex() {

        // Get index view
        final Index2View<V1, V2, T> iv = this.getIndex2View();

        // Build map and apply filtering
        IndexMap<V1, Index<V2, T>> indexMap = new IndexMap.OfIndex<V1, V2, T>(this.tx, iv);
        if (iv.hasFilters())
            indexMap = indexMap.filterKeys(new IndexKeyFilter(this.tx, iv, 1));

        // Done
        return indexMap;
    }

    @Override
    public CoreIndex<V1, V2> asIndex() {
        return new CoreIndex<V1, V2>(this.tx, this.getIndex2View().asIndexView());
    }
}

