
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a composite index.
 */
class CompositeIndexStorageInfo extends IndexStorageInfo {

    final List<Integer> storageIds;
    final List<FieldType<?>> fieldTypes;

    CompositeIndexStorageInfo(CompositeIndex index) {
        super(index.storageId);

        // Gather field storage ID's
        this.storageIds = new ArrayList<>(index.fields.size());
        index.fields.stream()
          .map(SimpleField::getStorageId)
          .forEach(this.storageIds::add);

        // Gather field types, genericized for indexing
        this.fieldTypes = new ArrayList<>(index.fields.size());
        index.fields.stream()
          .map(SimpleField::getFieldType)
          .map(FieldType::genericizeForIndex)
          .forEach(this.fieldTypes::add);
    }

    Object getIndex(Transaction tx) {
        switch (this.storageIds.size()) {
        case 2:
            return this.buildIndex(tx, this.fieldTypes.get(0), this.fieldTypes.get(1));
        case 3:
            return this.buildIndex(tx, this.fieldTypes.get(0), this.fieldTypes.get(1), this.fieldTypes.get(2));
        case 4:
            return this.buildIndex(tx, this.fieldTypes.get(0), this.fieldTypes.get(1), this.fieldTypes.get(2),
              this.fieldTypes.get(3));
        // COMPOSITE-INDEX
        default:
            throw new RuntimeException("internal error");
        }
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2> CoreIndex2<V1, V2, ObjId> buildIndex(Transaction tx,
      FieldType<V1> value1Type,
      FieldType<V2> value2Type) {
        return new CoreIndex2<>(tx, new Index2View<>(this.storageId,
          value1Type,
          value2Type,
          FieldTypeRegistry.OBJ_ID));
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2, V3> CoreIndex3<V1, V2, V3, ObjId> buildIndex(Transaction tx,
      FieldType<V1> value1Type,
      FieldType<V2> value2Type,
      FieldType<V3> value3Type) {
        return new CoreIndex3<>(tx, new Index3View<>(this.storageId,
          value1Type,
          value2Type,
          value3Type,
          FieldTypeRegistry.OBJ_ID));
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2, V3, V4> CoreIndex4<V1, V2, V3, V4, ObjId> buildIndex(Transaction tx,
      FieldType<V1> value1Type,
      FieldType<V2> value2Type,
      FieldType<V3> value3Type,
      FieldType<V4> value4Type) {
        return new CoreIndex4<>(tx, new Index4View<>(this.storageId,
          value1Type,
          value2Type,
          value3Type,
          value4Type,
          FieldTypeRegistry.OBJ_ID));
    }

// Object

    @Override
    public String toString() {
        return "composite index on fields " + this.fieldTypes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CompositeIndexStorageInfo that = (CompositeIndexStorageInfo)obj;
        return this.storageIds.equals(that.storageIds) && this.fieldTypes.equals(that.fieldTypes);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.storageIds.hashCode() ^ this.fieldTypes.hashCode();
    }
}

