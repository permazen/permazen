
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.schema.CounterSchemaField;

/**
 * Counter fields.
 *
 * <p>
 * Counter fields have {@code long} values and can be adjusted concurrently by multiple transactions,
 * typically without locking (depending on the underlying key/value store).
 * Counter fields do not support indexing or change listeners.
 *
 * <p>
 * Note: during {@link io.permazen.annotation.OnSchemaChange &#64;OnSchemaChange} notifications, counter field
 * values appear as {@code Long}s.
 */
public class CounterField extends Field<Long> {

    CounterField(ObjType objType, CounterSchemaField schemaField) {
        super(objType, schemaField, TypeToken.of(Long.class));
    }

// Public methods

    @Override
    public Long getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        return tx.readCounterField(id, this.name, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id) == 0;
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseCounterField(this);
    }

    @Override
    public String toString() {
        return "counter field \"" + this.name + "\"";
    }

// Package Methods

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        dstTx.writeCounterField(dstId, this.name, srcTx.readCounterField(srcId, this.name, false), false);
    }
}
