
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.schema.SchemaCompositeIndex;

/**
 * Represents a composite index on an {@link ObjType}.
 */
public class CompositeIndex extends Index {

// Constructor

    CompositeIndex(Schema schema, SchemaCompositeIndex index, ObjType objType, Iterable<? extends SimpleField<?>> fields) {
        super(schema, index, index.getName(), objType, fields);
        assert this.fields.size() >= 2;
        assert this.fields.stream().noneMatch(field -> field.parent != null);
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseCompositeIndex(this);
    }

// Object

    @Override
    public String toString() {
        return "composite " + super.toString();
    }

// Package Methods

    @Override
    public AbstractCoreIndex<ObjId> getIndex(Transaction tx) {
        switch (this.encodings.size()) {
        case 2:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1));
        case 3:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1), this.encodings.get(2));
        case 4:
            return this.buildIndex(tx, this.encodings.get(0), this.encodings.get(1), this.encodings.get(2), this.encodings.get(3));
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
}
