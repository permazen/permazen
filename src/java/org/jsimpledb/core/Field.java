
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

/**
 * Represents a field in an {@link ObjType} or a ({@linkplain SimpleField simple}) sub-field of a {@link ComplexField}.
 *
 * @param <T> Java type for the field's values
 */
public abstract class Field<T> extends SchemaItem {

    final TypeToken<T> typeToken;

    /**
     * Constructor for normal fields of an {@link ObjType}.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param typeToken Java type for the field's values
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    Field(String name, int storageId, SchemaVersion version, TypeToken<T> typeToken) {
        super(name, storageId, version);
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");
        this.typeToken = typeToken;
    }

    /**
     * Get the Java type corresponding to this field.
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    abstract boolean isEquivalent(Field<?> field);
}

