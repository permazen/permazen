
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.index.Index1;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Support superclass for the various {@link NavigableMap} views of indexes.
 */
abstract class IndexMap<K, V> extends EncodingMap<K, V> {

    // Primary constructor
    private IndexMap(KVStore kv, Encoding<K> keyType, ByteData prefix) {
        super(kv, keyType, true, prefix);
    }

    // Internal constructor
    private IndexMap(KVStore kv, Encoding<K> keyType, boolean reversed,
      ByteData prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(kv, keyType, true, reversed, prefix, keyRange, keyFilter, bounds);
    }

    public String getDescription() {
        return "IndexMap"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",keyType=" + this.keyEncoding
          + (this.bounds != null ? ",bounds=" + this.bounds : "")
          + (this.keyRange != null ? ",keyRange=" + this.keyRange : "")
          + (this.keyFilter != null ? ",keyFilter=" + this.keyFilter : "")
          + (this.reversed ? ",reversed" : "")
          + "]";
    }

// AbstractKVNavigableMap

    @Override
    public IndexMap<K, V> filterKeys(KeyFilter keyFilter) {
        return (IndexMap<K, V>)super.filterKeys(keyFilter);
    }

    @Override
    protected V decodeValue(KVPair pair) {
        assert pair.getKey().startsWith(this.prefix);
        final ByteData.Reader reader = pair.getKey().newReader();
        reader.skip(this.prefix.size());
        this.keyEncoding.skip(reader);
        return this.decodeValue(reader.dataReadSoFar());
    }

    /**
     * Decode index map value.
     *
     * @param keyPrefix the portion of the key encoding the corresponding map key
     */
    protected abstract V decodeValue(ByteData keyPrefix);

// OfValues

    /**
     * Implements {@link NavigableMap} views of indexes where the map values are {@link NavigableSet}s of target values.
     */
    static class OfValues<V, E> extends IndexMap<V, NavigableSet<E>> {

        private final Index1View<V, E> indexView;

        // Primary constructor
        OfValues(KVStore kv, Index1View<V, E> indexView) {
            super(kv, indexView.getValueEncoding(), indexView.prefix);
            this.indexView = indexView;
        }

        // Internal constructor
        private OfValues(KVStore kv, Index1View<V, E> indexView,
          boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<V> bounds) {
            super(kv, indexView.getValueEncoding(), reversed, indexView.prefix, keyRange, keyFilter, bounds);
            this.indexView = indexView;
        }

    // AbstractKVNavigableMap

        @Override
        protected NavigableMap<V, NavigableSet<E>> createSubMap(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<V> newBounds) {
            return new OfValues<>(this.kv, this.indexView, newReversed, newKeyRange, newKeyFilter, newBounds);
        }

    // IndexMap

        @Override
        protected NavigableSet<E> decodeValue(ByteData keyPrefix) {
            IndexSet<E> indexSet = new IndexSet<>(this.kv,
              this.indexView.getTargetEncoding(), this.indexView.prefixMode, keyPrefix);
            final KeyFilter targetFilter = this.indexView.getFilter(1);
            if (targetFilter != null) {
                indexSet = indexSet.filterKeys(new IndexKeyFilter(this.kv, keyPrefix,
                  new Encoding<?>[] { this.indexView.getTargetEncoding() }, new KeyFilter[] { targetFilter }, 1));
            }
            return indexSet;
        }
    }

// OfIndex

    /**
     * Implements {@link NavigableMap} views of composite indexes where the map values are of type {@link CoreIndex1}.
     */
    static class OfIndex1<V1, V2, T> extends IndexMap<V1, Index1<V2, T>> {

        private final Index2View<V1, V2, T> indexView;

        // Primary constructor
        OfIndex1(KVStore kv, Index2View<V1, V2, T> indexView) {
            super(kv, indexView.getValue1Encoding(), indexView.prefix);
            this.indexView = indexView;
        }

        // Internal constructor
        private OfIndex1(KVStore kv, Index2View<V1, V2, T> indexView,
          boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<V1> bounds) {
            super(kv, indexView.getValue1Encoding(), reversed, indexView.prefix, keyRange, keyFilter, bounds);
            this.indexView = indexView;
        }

    // AbstractKVNavigableMap

        @Override
        protected NavigableMap<V1, Index1<V2, T>> createSubMap(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<V1> newBounds) {
            return new OfIndex1<>(this.kv, this.indexView, newReversed, newKeyRange, newKeyFilter, newBounds);
        }

    // IndexMap

        @Override
        protected CoreIndex1<V2, T> decodeValue(ByteData keyPrefix) {
            return new CoreIndex1<>(this.kv, this.indexView.asIndex1View(keyPrefix));
        }
    }

// OfIndex2

    /**
     * Implements {@link NavigableMap} views of composite indexes where the map values are of type {@link CoreIndex2}.
     */
    static class OfIndex2<V1, V2, V3, T> extends IndexMap<V1, Index2<V2, V3, T>> {

        private final Index3View<V1, V2, V3, T> indexView;

        // Primary constructor
        OfIndex2(KVStore kv, Index3View<V1, V2, V3, T> indexView) {
            super(kv, indexView.getValue1Encoding(), indexView.prefix);
            this.indexView = indexView;
        }

        // Internal constructor
        private OfIndex2(KVStore kv, Index3View<V1, V2, V3, T> indexView,
          boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<V1> bounds) {
            super(kv, indexView.getValue1Encoding(), reversed, indexView.prefix, keyRange, keyFilter, bounds);
            this.indexView = indexView;
        }

    // AbstractKVNavigableMap

        @Override
        protected NavigableMap<V1, Index2<V2, V3, T>> createSubMap(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<V1> newBounds) {
            return new OfIndex2<>(this.kv, this.indexView, newReversed, newKeyRange, newKeyFilter, newBounds);
        }

    // IndexMap

        @Override
        protected CoreIndex2<V2, V3, T> decodeValue(ByteData keyPrefix) {
            return new CoreIndex2<>(this.kv, this.indexView.asIndex2View(keyPrefix));
        }
    }

// OfIndex3

    /**
     * Implements {@link NavigableMap} views of composite indexes where the map values are of type {@link CoreIndex3}.
     */
    static class OfIndex3<V1, V2, V3, V4, T> extends IndexMap<V1, Index3<V2, V3, V4, T>> {

        private final Index4View<V1, V2, V3, V4, T> indexView;

        // Primary constructor
        OfIndex3(KVStore kv, Index4View<V1, V2, V3, V4, T> indexView) {
            super(kv, indexView.getValue1Encoding(), indexView.prefix);
            this.indexView = indexView;
        }

        // Internal constructor
        private OfIndex3(KVStore kv, Index4View<V1, V2, V3, V4, T> indexView,
          boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<V1> bounds) {
            super(kv, indexView.getValue1Encoding(), reversed, indexView.prefix, keyRange, keyFilter, bounds);
            this.indexView = indexView;
        }

    // AbstractKVNavigableMap

        @Override
        protected NavigableMap<V1, Index3<V2, V3, V4, T>> createSubMap(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<V1> newBounds) {
            return new OfIndex3<>(this.kv, this.indexView, newReversed, newKeyRange, newKeyFilter, newBounds);
        }

    // IndexMap

        @Override
        protected CoreIndex3<V2, V3, V4, T> decodeValue(ByteData keyPrefix) {
            return new CoreIndex3<>(this.kv, this.indexView.asIndex3View(keyPrefix));
        }
    }
}
