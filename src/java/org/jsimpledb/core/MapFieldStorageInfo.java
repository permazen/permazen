
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Arrays;
import java.util.List;

import org.jsimpledb.util.ByteReader;

class MapFieldStorageInfo extends ComplexFieldStorageInfo {

    SimpleFieldStorageInfo keyField;
    SimpleFieldStorageInfo valueField;

    MapFieldStorageInfo(MapField<?, ?> field) {
        super(field);
    }

    @Override
    public List<SimpleFieldStorageInfo> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
    }

    void setSubFields(List<SimpleFieldStorageInfo> subFieldInfos) {
        if (subFieldInfos.size() != 2)
            throw new IllegalArgumentException();
        this.keyField = subFieldInfos.get(0);
        this.valueField = subFieldInfos.get(1);
    }

    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, ByteReader reader) {
        final FieldTypeMap<?, ?> fieldMap = (FieldTypeMap<?, ?>)tx.readMapField(referrer, this.storageId, false);
        if (storageId == this.keyField.storageId)
            fieldMap.remove(target);
        else if (storageId == this.valueField.storageId)
            fieldMap.remove(fieldMap.keyFieldType.read(reader));
        else
            throw new RuntimeException("internal error: storage ID " + storageId + " not found in " + this);
    }

    public SimpleFieldStorageInfo getKeyField() {
        return this.keyField;
    }

    public SimpleFieldStorageInfo getValueField() {
        return this.valueField;
    }

    @Override
    public String toString() {
        return "map field with key " + this.keyField + " and value " + this.valueField;
    }
}

