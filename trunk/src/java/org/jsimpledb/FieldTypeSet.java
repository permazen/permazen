
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Collections;
import java.util.Comparator;

import org.jsimpledb.kv.util.AbstractKVNavigableSet;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link AbstractKVNavigableSet} implementation that uses a {@link FieldType} to order and encode/decode elements.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 * </p>
 *
 * @param <E> element type
 */
abstract class FieldTypeSet<E> extends AbstractKVNavigableSet<E> {

    final Transaction tx;
    final FieldType<E> fieldType;
    final byte[] prefix;

    /**
     * Primary constructor.
     *
     * @param tx transaction
     * @param fieldType field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code fieldType} is null
     * @throws NullPointerException if {@code prefix} is null
     */
    FieldTypeSet(Transaction tx, FieldType<E> fieldType, boolean prefixMode, byte[] prefix) {
        this(tx, fieldType, prefixMode, false, prefix, prefix, ByteUtil.getKeyAfterPrefix(prefix), new Bounds<E>());
    }

    /**
     * Internal constructor.
     *
     * @param tx transaction
     * @param fieldType field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param minKey minimum visible key (inclusive), or null for none
     * @param maxKey maximum visible key (exclusive), or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code fieldType} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code minKey} or {@code maxKey} is out of range with respect to {@code prefix}
     */
    FieldTypeSet(Transaction tx, FieldType<E> fieldType, boolean prefixMode, boolean reversed,
      byte[] prefix, byte[] minKey, byte[] maxKey, Bounds<E> bounds) {
        super(tx.kvt, prefixMode, reversed, minKey, maxKey, bounds);
        if (fieldType == null)
            throw new IllegalArgumentException("null fieldType");
        if (prefix != null) {
            if (minKey != null && !ByteUtil.isPrefixOf(prefix, minKey))
                throw new IllegalArgumentException("minKey out of range with respect to prefix");
            if (maxKey != null && ByteUtil.compare(maxKey, ByteUtil.getKeyAfterPrefix(prefix)) > 0)
                throw new IllegalArgumentException("maxKey out of range with respect to prefix");
        }
        this.tx = tx;
        this.fieldType = fieldType;
        this.prefix = prefix;
    }

    @Override
    public final Comparator<? super E> comparator() {
        return this.reversed ? Collections.reverseOrder(this.fieldType) : this.fieldType;
    }

    @Override
    protected void encode(ByteWriter writer, Object obj) {
        if (this.prefix != null)
            writer.write(this.prefix);
        this.fieldType.validateAndWrite(writer, obj);
    }

    @Override
    protected E decode(ByteReader reader) {
        if (this.prefix != null) {
            assert ByteUtil.isPrefixOf(this.prefix, reader.getBytes());
            reader.skip(this.prefix.length);
        }
        return this.fieldType.read(reader);
    }
}

