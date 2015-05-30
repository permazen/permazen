
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.util.ByteReader;

class MapFieldStorageInfo<K, V> extends ComplexFieldStorageInfo<NavigableMap<K, V>> {

    final SimpleFieldStorageInfo<K> keyField;
    final SimpleFieldStorageInfo<V> valueField;

    MapFieldStorageInfo(MapField<K, V> field) {
        super(field);
        this.keyField = field.keyField.toStorageInfo();
        this.valueField = field.valueField.toStorageInfo();
    }

    @Override
    public List<SimpleFieldStorageInfo<?>> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
    }

    CoreIndex<K, ObjId> getKeyFieldIndex(Transaction tx) {
        return new CoreIndex<K, ObjId>(tx,
          new IndexView<K, ObjId>(this.keyField.storageId, this.keyField.fieldType, FieldTypeRegistry.OBJ_ID));
    }

    CoreIndex2<V, ObjId, K> getValueFieldIndex(Transaction tx) {
        return new CoreIndex2<V, ObjId, K>(tx,
          new Index2View<V, ObjId, K>(this.valueField.storageId,
           this.valueField.fieldType, FieldTypeRegistry.OBJ_ID, this.keyField.fieldType));
    }

    @Override
    CoreIndex<?, ObjId> getSimpleSubFieldIndex(Transaction tx, SimpleFieldStorageInfo<?> subField) {
        if (subField.equals(this.keyField))
            return this.getKeyFieldIndex(tx);
        if (subField.equals(this.valueField))
            return this.getValueFieldIndex(tx).asIndex();
        throw new RuntimeException("internal eror");
    }

    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, byte[] prefix) {
        final FieldTypeMap<?, ?> fieldMap = (FieldTypeMap<?, ?>)tx.readMapField(referrer, this.storageId, false);
        if (storageId == this.keyField.storageId)
            fieldMap.remove(target);
        else {
            assert storageId == this.valueField.storageId;
            for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
                final ByteReader reader = new ByteReader(i.next().getKey());
                reader.skip(prefix.length);
                fieldMap.remove(fieldMap.keyFieldType.read(reader));
            }
        }
    }

    public SimpleFieldStorageInfo<K> getKeyField() {
        return this.keyField;
    }

    public SimpleFieldStorageInfo<V> getValueField() {
        return this.valueField;
    }

// Object

    @Override
    public String toString() {
        return "map field with key " + this.keyField + " and value " + this.valueField;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapFieldStorageInfo<?, ?> that = (MapFieldStorageInfo<?, ?>)obj;
        return this.keyField.equals(that.keyField) && this.valueField.equals(that.valueField);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyField.hashCode() ^ this.valueField.hashCode();
    }
}

