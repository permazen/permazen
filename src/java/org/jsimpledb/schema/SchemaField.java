
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * A field in a {@link SchemaObjectType}.
 */
public abstract class SchemaField extends AbstractSchemaItem {

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visitor return type
     * @return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(SchemaFieldSwitch<R> target);

// Object

    @Override
    public String toString() {
        return "field " + super.toString();
    }

// Cloneable

    @Override
    public SchemaField clone() {
        return (SchemaField)super.clone();
    }
}

