
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.core.util.ObjIdMap;

/**
 * Counter fields.
 *
 * <p>
 * Counter fields have {@code long} values and can be adjusted concurrently by multiple transactions,
 * typically without locking (depending on the underlying key/value store).
 * Counter fields do not support indexing or change listeners.
 *
 * <p>
 * Note: during schema version change notification, counter field values appear as plain {@code long} values.
 */
public class CounterField extends Field<Long> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is zero or less
     */
    CounterField(String name, int storageId, Schema schema) {
        super(name, storageId, schema, TypeToken.of(Long.class));
    }

// Public methods

    @Override
    public Long getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        return tx.readCounterField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id) == 0;
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseCounterField(this);
    }

    @Override
    public String toString() {
        return "counter field `" + this.name + "'";
    }

// Non-public methods

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        dstTx.writeCounterField(dstId, this.storageId, srcTx.readCounterField(srcId, this.storageId, false), false);
    }

    // Counter fields are never indexed
    @Override
    StorageInfo toStorageInfo() {
        return null;
    }

    @Override
    boolean isUpgradeCompatible(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        return true;
    }
}

