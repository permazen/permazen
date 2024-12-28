
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.kv.util.AbstractKVNavigableMap;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Collections;
import java.util.Comparator;

/**
 * {@link AbstractKVNavigableMap} implementation that uses an {@link Encoding} to order and encode/decode keys.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class EncodingMap<K, V> extends AbstractKVNavigableMap<K, V> {

    final Encoding<K> keyEncoding;
    final ByteData prefix;

    /**
     * Primary constructor.
     *
     * @param kv key/value data
     * @param keyEncoding key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code keyEncoding} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    EncodingMap(KVStore kv, Encoding<K> keyEncoding, boolean prefixMode, ByteData prefix) {
        this(kv, keyEncoding, prefixMode, false, prefix, KeyRange.forPrefix(prefix), null, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param kv key/value data
     * @param keyEncoding key encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param keyRange key range restriction; must at least restrict to {@code prefix}
     * @param keyFilter key filter restriction, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code keyEncoding} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code keyRange} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code prefix} is not null but {@code keyRange} does not restrict within {@code prefix}
     */
    EncodingMap(KVStore kv, Encoding<K> keyEncoding, boolean prefixMode, boolean reversed,
      ByteData prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(kv, prefixMode, reversed, keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyEncoding != null, "null keyEncoding");
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        if (!KeyRange.forPrefix(prefix).contains(keyRange)) {
            throw new IllegalArgumentException(String.format(
              "%s does not restrict to prefix %s", keyRange, ByteUtil.toString(prefix)));
        }
        this.keyEncoding = keyEncoding;
        this.prefix = prefix;
    }

    @Override
    public final Comparator<? super K> comparator() {
        return this.reversed ? Collections.reverseOrder(this.keyEncoding) : this.keyEncoding;
    }

    @Override
    protected void encodeKey(ByteData.Writer writer, Object obj) {
        writer.write(this.prefix);
        this.keyEncoding.validateAndWrite(writer, obj);
    }

    @Override
    protected K decodeKey(ByteData.Reader reader) {
        assert reader.dataNotYetRead().startsWith(this.prefix);
        reader.skip(this.prefix.size());
        return this.keyEncoding.read(reader);
    }
}
