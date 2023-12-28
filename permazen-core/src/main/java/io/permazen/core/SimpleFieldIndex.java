
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.schema.SimpleSchemaField;

import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An index on a simple field that is not a sub-field of a complex field.
 *
 * @param <T> field's value type
 */
public class SimpleFieldIndex<T> extends SimpleIndex<T> {

// Constructor

    SimpleFieldIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, SimpleField<T> field) {
        super(schema, schemaField, schemaField.getName(), objType, field);
        assert field.parent == null;
    }

// Public Methods

    @Override
    public CoreIndex1<T, ObjId> getIndex(Transaction tx) {
        return new CoreIndex1<>(tx.kvt, new Index1View<>(this.storageId, this.getField().getEncoding(), Encodings.OBJ_ID));
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseSimpleFieldIndex(this);
    }

// Package Methods

    @Override
    void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers) {
        assert this.getField() instanceof ReferenceField;
        for (ObjId referrer : referrers)
            tx.writeSimpleField(referrer, this.name, null, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    void readAllNonNull(Transaction tx, ObjId target, Set<T> values, Predicate<? super T> filter) {
        final T value = (T)tx.readSimpleField(target, this.name, false);
        if (value != null && (filter == null || filter.test(value)))
            values.add(value);
    }
}
