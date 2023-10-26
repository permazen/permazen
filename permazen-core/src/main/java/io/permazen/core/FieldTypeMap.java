
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.kv.util.AbstractKVNavigableMap;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.Collections;
import java.util.Comparator;

/**
 * {@link AbstractKVNavigableMap} implementation that uses a {@link FieldType} to order and encode/decode keys.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class FieldTypeMap<K, V> extends AbstractKVNavigableMap<K, V> {

    final FieldType<K> keyFieldType;
    final byte[] prefix;

    /**
     * Primary constructor.
     *
     * @param kv key/value data
     * @param keyFieldType key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code keyFieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    FieldTypeMap(KVStore kv, FieldType<K> keyFieldType, boolean prefixMode, byte[] prefix) {
        this(kv, keyFieldType, prefixMode, false, prefix, KeyRange.forPrefix(prefix), null, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param kv key/value data
     * @param keyFieldType key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param keyRange key range restriction; must at least restrict to {@code prefix}
     * @param keyFilter key filter restriction, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code keyFieldType} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code keyRange} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code prefix} is not null but {@code keyRange} does not restrict within {@code prefix}
     */
    FieldTypeMap(KVStore kv, FieldType<K> keyFieldType, boolean prefixMode, boolean reversed,
      byte[] prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(kv, prefixMode, reversed, keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyFieldType != null, "null keyFieldType");
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        if (!KeyRange.forPrefix(prefix).contains(keyRange))
            throw new IllegalArgumentException(keyRange + " does not restrict to prefix " + ByteUtil.toString(prefix));
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

