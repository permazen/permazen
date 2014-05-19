
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Superclass of fields with types assignable to {@link Collection}.
 *
 * @param <C> Java collection type
 * @param <E> Java collection element type
 */
public abstract class CollectionField<C extends Collection<E>, E> extends ComplexField<C> {

    public static final String ELEMENT_FIELD_NAME = "element";

    final SimpleField<E> elementField;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field content storage ID
     * @param typeToken Java type for the field's values
     * @param version schema version
     * @param elementField this field's element sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    CollectionField(String name, int storageId, SchemaVersion version, TypeToken<C> typeToken, SimpleField<E> elementField) {
        super(name, storageId, version, typeToken);
        if (elementField == null)
            throw new IllegalArgumentException("null elementField");
        this.elementField = elementField;
        assert this.elementField.parent == null;
        this.elementField.parent = this;
    }

// Public methods

    /**
     * Get the element field.
     */
    public SimpleField<E> getElementField() {
        return this.elementField;
    }

    @Override
    public final List<SimpleField<E>> getSubFields() {
        return Collections.<SimpleField<E>>singletonList(this.elementField);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id).isEmpty();
    }

// Non-public methods

    @Override
    abstract CollectionFieldStorageInfo toStorageInfo();
}

