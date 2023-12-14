
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SimpleSchemaField;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

abstract class CollectionElementIndex<C extends Collection<E>, E> extends ComplexSubFieldIndex<C, E> {

// Constructor

    CollectionElementIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, CollectionField<C, E> field) {
        super(schema, schemaField, objType, field, field.getElementField());
    }

// Package Methods

    @Override
    void readAllNonNull(Transaction tx, ObjId id, Set<E> values, Predicate<? super E> filter) {
        for (E value : this.parentRepresentative.getValueInternal(tx, id)) {
            if (value != null && (filter == null || filter.test(value)))
                values.add(value);
        }
    }
}
