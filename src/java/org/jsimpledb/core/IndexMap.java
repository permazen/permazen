
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Implements the {@link NavigableMap} view of an index.
 *
 * @param <V> type of the values being indexed
 * @param <E> type of the index entries associated with each indexed value
 */
class IndexMap<V, E> extends FieldTypeMap<V, NavigableSet<E>> {

    private final FieldType<E> entryType;

    /**
     * Constructor for field indexes.
     */
    IndexMap(Transaction tx, int storageId, FieldType<V> valueType, FieldType<E> entryType) {
        this(tx, UnsignedIntEncoder.encode(storageId), valueType, entryType);
    }

    /**
     * Primary constructor.
     */
    IndexMap(Transaction tx, byte[] prefix, FieldType<V> valueType, FieldType<E> entryType) {
        super(tx, valueType, true, prefix);
        this.entryType = entryType;
    }

    /**
     * Internal constructor.
     */
    private IndexMap(Transaction tx, byte[] prefix, FieldType<V> valueType, FieldType<E> entryType,
      boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<V> bounds) {
        super(tx, valueType, true, reversed, prefix, keyRange, keyFilter, bounds);
        this.entryType = entryType;
    }

    @Override
    protected NavigableMap<V, NavigableSet<E>> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<V> newBounds) {
        return new IndexMap<V, E>(this.tx, this.prefix, this.keyFieldType,
          this.entryType, newReversed, newKeyRange, newKeyFilter, newBounds);
    }

    @Override
    protected NavigableSet<E> decodeValue(KVPair pair) {
        final ByteReader reader = new ByteReader(pair.getKey());
        assert ByteUtil.isPrefixOf(this.prefix, reader.getBytes());
        reader.skip(this.prefix.length);
        this.keyFieldType.skip(reader);
        return new IndexSet(reader.getBytes(0, reader.getOffset()));
    }

// IndexSet

    /**
     * Implements the {@link NavigableSet} view of all entries associated with a specific value of an indexed simple field.
     */
    class IndexSet extends FieldTypeSet<E> {

        /**
         * Primary constructor.
         *
         * @param prefix index entries prefix (includes field value)
         */
        IndexSet(byte[] prefix) {
            super(IndexMap.this.tx, IndexMap.this.entryType, true, prefix);
        }

        /**
         * Internal constructor.
         *
         * @param prefixMode whether to allow keys to have trailing garbage
         * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
         * @param prefix prefix of all keys
         * @param keyRange key range restrictions; must at least restrict to {@code prefix}
         * @param keyFilter key filter restriction, or null for none
         * @param bounds range restriction
         */
        private IndexSet(boolean prefixMode, boolean reversed,
          byte[] prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
            super(IndexMap.this.tx, IndexMap.this.entryType, prefixMode, reversed, prefix, keyRange, keyFilter, bounds);
        }

        /**
         * Restrict this instance so that it only contains objects of the specified type.
         *
         * @param storageId object type storage ID
         */
        @SuppressWarnings("unchecked")
        public IndexSet forObjType(int storageId) {
            final ByteWriter writer = new ByteWriter();
            writer.write(this.prefix);
            UnsignedIntEncoder.write(writer, storageId);
            final byte[] minKey = writer.getBytes();
            final byte[] maxKey = ByteUtil.getKeyAfterPrefix(minKey);
            return (IndexMap<V, E>.IndexSet)this.filter(new KeyRanges(minKey, maxKey));
        }

        @Override
        protected NavigableSet<E> createSubSet(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds) {
            return new IndexSet(this.prefixMode, newReversed, this.prefix, newKeyRange, newKeyFilter, newBounds);
        }
    }
}

