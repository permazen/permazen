
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a composite index.
 */
class CompositeIndexStorageInfo extends IndexStorageInfo {

    final List<Integer> storageIds;
    final List<Encoding<?>> encodings;

    CompositeIndexStorageInfo(CompositeIndex index) {
        super(index.storageId);

        // Gather field storage ID's
        this.storageIds = new ArrayList<>(index.fields.size());
        index.fields.stream()
          .map(SimpleField::getStorageId)
          .forEach(this.storageIds::add);

        // Gather encodings, genericized for indexing
        this.encodings = new ArrayList<>(index.fields.size());
        index.fields.stream()
          .map(SimpleField::getEncoding)
          .map(Encoding::genericizeForIndex)
          .forEach(this.encodings::add);
    }

    Object getIndex(Transaction tx) {
        switch (this.storageIds.size()) {
        case 2:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1));
        case 3:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1), this.encodings.get(2));
        case 4:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1), this.encodings.get(2),
              this.encodings.get(3));
        // COMPOSITE-INDEX
        default:
            throw new RuntimeException("internal error");
        }
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2> CoreIndex2<V1, V2, ObjId> buildIndex(Transaction tx,
      Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding) {
        return new CoreIndex2<>(tx.kvt, new Index2View<>(this.storageId,
          value1Encoding,
          value2Encoding,
          Encodings.OBJ_ID));
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2, V3> CoreIndex3<V1, V2, V3, ObjId> buildIndex(Transaction tx,
      Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding,
      Encoding<V3> value3Encoding) {
        return new CoreIndex3<>(tx.kvt, new Index3View<>(this.storageId,
          value1Encoding,
          value2Encoding,
          value3Encoding,
          Encodings.OBJ_ID));
    }

    // This method exists solely to bind the generic type parameters
    private <V1, V2, V3, V4> CoreIndex4<V1, V2, V3, V4, ObjId> buildIndex(Transaction tx,
      Encoding<V1> value1Encoding,
      Encoding<V2> value2Encoding,
      Encoding<V3> value3Encoding,
      Encoding<V4> value4Encoding) {
        return new CoreIndex4<>(tx.kvt, new Index4View<>(this.storageId,
          value1Encoding,
          value2Encoding,
          value3Encoding,
          value4Encoding,
          Encodings.OBJ_ID));
    }

// Object

    @Override
    public String toString() {
        return "composite index on fields " + this.encodings;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CompositeIndexStorageInfo that = (CompositeIndexStorageInfo)obj;
        return this.storageIds.equals(that.storageIds) && this.encodings.equals(that.encodings);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.storageIds.hashCode() ^ this.encodings.hashCode();
    }
}
