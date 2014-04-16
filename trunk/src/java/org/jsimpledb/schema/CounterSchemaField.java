
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * A counter field in a {@link SchemaObject}.
 */
public class CounterSchemaField extends SchemaField {

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseCounterSchemaField(this);
    }

// Object

    @Override
    public String toString() {
        return "counter " + super.toString();
    }

// Cloneable

    @Override
    public CounterSchemaField clone() {
        return (CounterSchemaField)super.clone();
    }
}

