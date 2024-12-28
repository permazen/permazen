
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.kv.util.AbstractKVNavigableSet;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Collections;
import java.util.Comparator;

/**
 * {@link AbstractKVNavigableSet} implementation that uses an {@link Encoding} to order and encode/decode elements.
 *
 * <p>
 * This class provides a read-only implementation; subclasses can override as appropriate to make the set mutable.
 *
 * @param <E> element type
 */
abstract class EncodingSet<E> extends AbstractKVNavigableSet<E> {

    final Encoding<E> encoding;
    final ByteData prefix;

    /**
     * Primary constructor.
     *
     * @param kv key/value data
     * @param encoding field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix implicit prefix of all keys
     * @throws IllegalArgumentException if {@code encoding} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    EncodingSet(KVStore kv, Encoding<E> encoding, boolean prefixMode, ByteData prefix) {
        this(kv, encoding, prefixMode, false, prefix, KeyRange.forPrefix(prefix), null, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param kv key/value data
     * @param encoding field encoder/decoder
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix implicit prefix of all keys, or null for none
     * @param keyRange key range restriction; must at least restrict to {@code prefix}
     * @param keyFilter key filter restriction, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code encoding} is null
     * @throws IllegalArgumentException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code keyRange} is null
     * @throws IllegalArgumentException if {@code bounds} is null
     * @throws IllegalArgumentException if {@code prefix} is not null but {@code keyRange} does not restrict within {@code prefix}
     */
    EncodingSet(KVStore kv, Encoding<E> encoding, boolean prefixMode, boolean reversed,
      ByteData prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(kv, prefixMode, reversed, keyRange, keyFilter, bounds);
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(prefix.isEmpty() || keyRange != null, "null keyRange");
        if (keyRange != null && !KeyRange.forPrefix(prefix).contains(keyRange)) {
            throw new IllegalArgumentException(String.format(
              "%s does not restrict to prefix %s", keyRange, ByteUtil.toString(prefix)));
        }
        this.encoding = encoding;
        this.prefix = prefix;
    }

    @Override
    public final Comparator<? super E> comparator() {
        return this.reversed ? Collections.reverseOrder(this.encoding) : this.encoding;
    }

    @Override
    protected void encode(ByteData.Writer writer, Object obj) {
        writer.write(this.prefix);
        this.encoding.validateAndWrite(writer, obj);
    }

    @Override
    protected E decode(ByteData.Reader reader) {
        assert reader.dataNotYetRead().startsWith(this.prefix);
        reader.skip(this.prefix.size());
        return this.encoding.read(reader);
    }
}
