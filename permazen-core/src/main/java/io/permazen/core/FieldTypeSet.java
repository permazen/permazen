
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.kv.util.AbstractKVNavigableSet;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.Collections;
import java.util.Comparator;

/**
 * {@link AbstractKVNavigableSet} implementation that uses a {@link FieldType} to order and encode/decode elements.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 *
 * @param <E> element type
 */
abstract class FieldTypeSet<E> extends AbstractKVNavigableSet<E> {

    final FieldType<E> fieldType;
    final byte[] prefix;

    /**
     * Primary constructor.
     *
     * @param kv key/value data
     * @param fieldType field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code fieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    FieldTypeSet(KVStore kv, FieldType<E> fieldType, boolean prefixMode, byte[] prefix) {
        this(kv, fieldType, prefixMode, false, prefix, KeyRange.forPrefix(prefix), null, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param kv key/value data
     * @param fieldType field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param keyRange key range restriction; must at least restrict to {@code prefix}
     * @param keyFilter key filter restriction, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code fieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code keyRange} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code prefix} is not null but {@code keyRange} does not restrict within {@code prefix}
     */
    FieldTypeSet(KVStore kv, FieldType<E> fieldType, boolean prefixMode, boolean reversed,
      byte[] prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(kv, prefixMode, reversed, keyRange, keyFilter, bounds);
        Preconditions.checkArgument(fieldType != null, "null fieldType");
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(prefix.length == 0 || keyRange != null, "null keyRange");
        if (keyRange != null && !KeyRange.forPrefix(prefix).contains(keyRange))
            throw new IllegalArgumentException(keyRange + " does not restrict to prefix " + ByteUtil.toString(prefix));
        this.fieldType = fieldType;
        this.prefix = prefix;
    }

    @Override
    public final Comparator<? super E> comparator() {
        return this.reversed ? Collections.reverseOrder(this.fieldType) : this.fieldType;
    }

    @Override
    protected void encode(ByteWriter writer, Object obj) {
        writer.write(this.prefix);
        this.fieldType.validateAndWrite(writer, obj);
    }

    @Override
    protected E decode(ByteReader reader) {
        assert ByteUtil.isPrefixOf(this.prefix, reader.getBytes());
        reader.skip(this.prefix.length);
        return this.fieldType.read(reader);
    }
}

