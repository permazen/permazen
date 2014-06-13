
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

/**
 * Counter fields.
 *
 * <p>
 * Counter fields have {@code long} values and support lock-free addition/subtraction.
 * Counter fields do not support indexing or change listeners.
 * </p>
 *
 * <p>
 * Supported in {@link org.jsimpledb.kv.KVDatabase}s whose {@linkplain org.jsimpledb.kv.KVDatabase#createTransaction transactions}
 * implement the {@link org.jsimpledb.kv.CountingKVStore} interface.
 * </p>
 *
 * <p>
 * If you access a counter field while running on a non-supporting database,
 * an {@link UnsupportedOperationException} will be thrown.
 * </p>
 *
 * <p>
 * During version change notification, counter fields appear as plain {@code long} values.
 * </p>
 */
public class CounterField extends Field<Long> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is zero or less
     */
    CounterField(String name, int storageId, SchemaVersion version) {
        super(name, storageId, version, TypeToken.of(Long.class));
    }

    @Override
    CounterFieldStorageInfo toStorageInfo() {
        return new CounterFieldStorageInfo(this);
    }

// Public methods

    @Override
    public Long getValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return tx.readCounterField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id) == 0;
    }

    @Override
    public String toString() {
        return "counter field `" + this.name + "'";
    }

// Non-public methods

    @Override
    boolean isEquivalent(Field<?> field) {
        return field.getClass() == this.getClass();
    }
}

