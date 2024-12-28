
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyFilter;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A view of an index on a single value.
 *
 * @param <V> indexed value type
 * @param <T> target type
 */
class Index1View<V, T> extends AbstractIndexView {

    /**
     * Normal constructor.
     *
     * @param storageId field storage ID
     * @param valueEncoding index value encoding
     * @param targetEncoding index target encoding
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index1View(int storageId, Encoding<V> valueEncoding, Encoding<T> targetEncoding) {
        this(UnsignedIntEncoder.encode(storageId), false, valueEncoding, targetEncoding);
    }

    /**
     * Constructor for views formed from larger composite indexes.
     *
     * @param prefix key prefix
     * @param prefixMode true if {@code targetEncoding} is not the final field in the index
     * @param valueEncoding index value encoding
     * @param targetEncoding index target encoding
     * @throws IllegalArgumentException if any parameter is null is null or empty
     */
    Index1View(ByteData prefix, boolean prefixMode, Encoding<V> valueEncoding, Encoding<T> targetEncoding) {
        super(prefix, prefixMode, valueEncoding, targetEncoding);
    }

    // Internal copy constructor
    private Index1View(Index1View<V, T> original) {
        super(original);
    }

    @SuppressWarnings("unchecked")
    public Encoding<V> getValueEncoding() {
        return (Encoding<V>)this.encodings[0];
    }

    @SuppressWarnings("unchecked")
    public Encoding<T> getTargetEncoding() {
        return (Encoding<T>)this.encodings[1];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Index1View<V, T> filter(int index, KeyFilter keyFilter) {
        return (Index1View<V, T>)super.filter(index, keyFilter);
    }

    @Override
    protected Index1View<V, T> copy() {
        return new Index1View<>(this);
    }
}
