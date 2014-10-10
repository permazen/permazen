
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Arrays;
import java.util.List;

import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.util.ByteReader;

class MapFieldStorageInfo extends ComplexFieldStorageInfo {

    final SimpleFieldStorageInfo keyField;
    final SimpleFieldStorageInfo valueField;

    MapFieldStorageInfo(MapField<?, ?> field) {
        super(field);
        this.keyField = field.keyField.toStorageInfo();
        this.valueField = field.valueField.toStorageInfo();
    }

    @Override
    public List<SimpleFieldStorageInfo> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
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

    public SimpleFieldStorageInfo getKeyField() {
        return this.keyField;
    }

    public SimpleFieldStorageInfo getValueField() {
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
        final MapFieldStorageInfo that = (MapFieldStorageInfo)obj;
        return this.keyField.equals(that.keyField) && this.valueField.equals(that.valueField);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyField.hashCode() ^ this.valueField.hashCode();
    }
}

