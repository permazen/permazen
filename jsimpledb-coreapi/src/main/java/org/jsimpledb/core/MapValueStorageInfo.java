
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Set;

import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.util.ByteReader;

class MapValueStorageInfo<K, V> extends ComplexSubFieldStorageInfo<V, MapField<K, V>> {

    final FieldType<K> keyFieldType;

    MapValueStorageInfo(MapField<K, V> field) {
        super(field.valueField, field);
        this.keyFieldType = field.keyField.fieldType.genericizeForIndex();
    }

    CoreIndex2<V, ObjId, K> getValueIndex(Transaction tx) {
        return new CoreIndex2<>(tx.kvt,
          new Index2View<>(this.storageId, this.fieldType, FieldTypeRegistry.OBJ_ID, this.keyFieldType));
    }

    @Override
    CoreIndex<V, ObjId> getIndex(Transaction tx) {
        return this.getValueIndex(tx).asIndex();
    }

    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        final FieldTypeMap<?, ?> fieldMap
          = (FieldTypeMap<?, ?>)tx.readMapField(referrer, this.parentRepresentative.storageId, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteReader reader = new ByteReader(i.next().getKey());
            reader.skip(prefix.length);
            fieldMap.remove(fieldMap.keyFieldType.read(reader));
        }
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId target, Set<V> values) {
        for (V value : this.parentRepresentative.getValueInternal(tx, target).values()) {
            if (value != null)
                values.add(value);
        }
    }

// Object

    @Override
    public String toString() {
        return "map value field with " + this.fieldType;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapValueStorageInfo<?, ?> that = (MapValueStorageInfo<?, ?>)obj;
        return this.keyFieldType.equals(that.keyFieldType);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyFieldType.hashCode();
    }
}

