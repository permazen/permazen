
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVPairIterator;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An index on the values of a map field.
 *
 * @param <K> map key type
 * @param <V> map value type
 */
public class MapValueIndex<K, V> extends ComplexSubFieldIndex<NavigableMap<K, V>, V> {

    private final Encoding<K> keyFieldEncoding;

// Constructor

    MapValueIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, MapField<K, V> field) {
        super(schema, schemaField, objType, field, field.getValueField());
        this.keyFieldEncoding = field.keyField.encoding;
    }

// Public Methods

    /**
     * Get this index's view of the given transaction, including entry key.
     *
     * <p>
     * The returned index includes the map values and their associated keys.
     *
     * @param tx transaction
     * @return view of this index in {@code tx}
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public CoreIndex2<V, ObjId, K> getValueIndex(Transaction tx) {
        return new CoreIndex2<>(tx.kvt,
          new Index2View<>(this.storageId, this.getEncoding(), Encodings.OBJ_ID, this.keyFieldEncoding));
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseMapValueIndex(this);
    }

// Package Methods

    @Override
    boolean isPrefixModeForIndex() {
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    void unreference(Transaction tx, boolean remove, ObjId target, ObjId referrer, ByteData prefix) {
        final EncodingMap<?, ?> fieldMap = (EncodingMap<?, ?>)tx.readMapField(referrer, this.getField().parent.name, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteData.Reader reader = i.next().getKey().newReader();
            reader.skip(prefix.size());
            final Object key = fieldMap.keyEncoding.read(reader);
            if (remove)
                fieldMap.remove(key);
            else
                ((EncodingMap<Object, ?>)fieldMap).put(key, null);
        }
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId target, Set<V> values, Predicate<? super V> filter) {
        for (V value : this.parentRepresentative.getValueInternal(tx, target).values()) {
            if (value != null && (filter == null || filter.test(value)))
                values.add(value);
        }
    }
}
