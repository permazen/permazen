
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;

import io.permazen.core.type.ReferenceFieldType;

/**
 * Represents an index on a regular simple field (not a sub-field of a complex field).
 */
class RegularSimpleFieldStorageInfo<T> extends SimpleFieldStorageInfo<T> {

    RegularSimpleFieldStorageInfo(SimpleField<T> field) {
        super(field);
        assert field.parent == null;
    }

    @Override
    void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers) {
        assert this.fieldType instanceof ReferenceFieldType;
        for (ObjId referrer : referrers)
            tx.writeSimpleField(referrer, this.storageId, null, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    void readAllNonNull(Transaction tx, ObjId target, Set<T> values, Predicate<? super T> filter) {
        final T value = (T)tx.readSimpleField(target, this.storageId, false);
        if (value != null && (filter == null || filter.test(value)))
            values.add(value);
    }

// Object

    @Override
    public String toString() {
        return "simple field with " + this.fieldType;
    }
}

