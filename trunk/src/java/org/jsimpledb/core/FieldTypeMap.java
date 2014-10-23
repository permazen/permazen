
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Collections;
import java.util.Comparator;

import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.AbstractKVNavigableMap;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link AbstractKVNavigableMap} implementation that uses a {@link FieldType} to order and encode/decode keys.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class FieldTypeMap<K, V> extends AbstractKVNavigableMap<K, V> {

    final Transaction tx;
    final FieldType<K> keyFieldType;
    final byte[] prefix;

    /**
     * Primary constructor.
     *
     * @param tx transaction
     * @param keyFieldType key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code keyFieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    FieldTypeMap(Transaction tx, FieldType<K> keyFieldType, boolean prefixMode, byte[] prefix) {
        this(tx, keyFieldType, prefixMode, false, prefix, KeyRanges.forPrefix(prefix), new Bounds<K>());
    }

    /**
     * Internal constructor.
     *
     * @param tx transaction
     * @param keyFieldType key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param keyRanges key range restrictions; must at least restrict to {@code prefix}
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code keyFieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code keyRanges} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code prefix} is not null but {@code keyRanges} does not restrict within {@code prefix}
     */
    FieldTypeMap(Transaction tx, FieldType<K> keyFieldType, boolean prefixMode, boolean reversed,
      byte[] prefix, KeyRanges keyRanges, Bounds<K> bounds) {
        super(tx.kvt, prefixMode, reversed, keyRanges, bounds);
        if (keyFieldType == null)
            throw new IllegalArgumentException("null keyFieldType");
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        if (keyRanges == null)
            throw new IllegalArgumentException("null keyRanges");
        if (!KeyRanges.forPrefix(prefix).contains(keyRanges))
            throw new IllegalArgumentException(keyRanges + " does not restrict to prefix " + ByteUtil.toString(prefix));
        this.tx = tx;
        this.keyFieldType = keyFieldType;
        this.prefix = prefix;
    }

    @Override
    public final Comparator<? super K> comparator() {
        return this.reversed ? Collections.reverseOrder(this.keyFieldType) : this.keyFieldType;
    }

    @Override
    protected void encodeKey(ByteWriter writer, Object obj) {
        writer.write(this.prefix);
        this.keyFieldType.validateAndWrite(writer, obj);
    }

    @Override
    protected K decodeKey(ByteReader reader) {
        assert ByteUtil.isPrefixOf(this.prefix, reader.getBytes());
        reader.skip(this.prefix.length);
        return this.keyFieldType.read(reader);
    }
}

