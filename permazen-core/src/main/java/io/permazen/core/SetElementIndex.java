
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.NavigableSet;

/**
 * An index on the elements of a set field.
 *
 * @param <E> set element type
 */
public class SetElementIndex<E> extends CollectionElementIndex<NavigableSet<E>, E> {

// Constructor

    SetElementIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, SetField<E> field) {
        super(schema, schemaField, objType, field);
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseSetElementIndex(this);
    }

// Package Methods

    @Override
    boolean isPrefixModeForIndex() {
        return false;
    }

    @Override
    void unreference(Transaction tx, boolean remove, ObjId target, ObjId referrer, ByteData prefix) {
        final NavigableSet<?> set = tx.readSetField(referrer, this.getField().parent.name, false);
        set.remove(target);
        if (!remove)
            set.add(null);
    }
}
