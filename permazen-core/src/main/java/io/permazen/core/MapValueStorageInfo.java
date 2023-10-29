
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.Encoding;
import io.permazen.kv.KVPairIterator;
import io.permazen.util.ByteReader;

import java.util.Set;
import java.util.function.Predicate;

class MapValueStorageInfo<K, V> extends ComplexSubFieldStorageInfo<V, MapField<K, V>> {

    final Encoding<K> keyEncoding;

    MapValueStorageInfo(MapField<K, V> field) {
        super(field.valueField, field);
        this.keyEncoding = field.keyField.encoding.genericizeForIndex();
    }

    CoreIndex2<V, ObjId, K> getValueIndex(Transaction tx) {
        return new CoreIndex2<>(tx.kvt,
          new Index2View<>(this.storageId, this.encoding, Encodings.OBJ_ID, this.keyEncoding));
    }

    @Override
    CoreIndex<V, ObjId> getIndex(Transaction tx) {
        return this.getValueIndex(tx).asIndex();
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        final EncodingMap<?, ?> fieldMap
          = (EncodingMap<?, ?>)tx.readMapField(referrer, this.parentRepresentative.storageId, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteReader reader = new ByteReader(i.next().getKey());
            reader.skip(prefix.length);
            fieldMap.remove(fieldMap.keyEncoding.read(reader));
        }
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId target, Set<V> values, Predicate<? super V> filter) {
        for (V value : this.parentRepresentative.getValueInternal(tx, target).values()) {
            if (value != null && (filter == null || filter.test(value)))
                values.add(value);
        }
    }

// Object

    @Override
    public String toString() {
        return "map value field with " + this.encoding;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapValueStorageInfo<?, ?> that = (MapValueStorageInfo<?, ?>)obj;
        return this.keyEncoding.equals(that.keyEncoding);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyEncoding.hashCode();
    }
}
