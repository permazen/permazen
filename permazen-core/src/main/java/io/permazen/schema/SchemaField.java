
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

/**
 * A field in a {@link SchemaObjectType}.
 */
public abstract class SchemaField extends AbstractObjectTypeMember {

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
        return "field " + this.toStringName();
    }

// Cloneable

    @Override
    public SchemaField clone() {
        return (SchemaField)super.clone();
    }
}
