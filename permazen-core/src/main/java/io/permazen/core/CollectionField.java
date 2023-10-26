
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

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
     * @param schema schema version
     * @param elementField this field's element sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    CollectionField(String name, int storageId, Schema schema, TypeToken<C> typeToken, SimpleField<E> elementField) {
        super(name, storageId, schema, typeToken);
        Preconditions.checkArgument(elementField != null, "null elementField");
        this.elementField = elementField;
        assert this.elementField.parent == null;
        this.elementField.parent = this;
    }

// Public methods

    /**
     * Get the element field.
     *
     * @return collection element field
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

    @Override
    public abstract C getValue(Transaction tx, ObjId id);

// Non-public methods

    @Override
    @SuppressWarnings("unchecked")
    <F> Iterable<F> iterateSubField(Transaction tx, ObjId id, SimpleField<F> subField) {
        if (subField == this.elementField)
            return (Iterable<F>)this.getValue(tx, id);
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    void unreferenceRemovedTypes(Transaction tx, ObjId id, ReferenceField subField, SortedSet<Integer> removedStorageIds) {
        assert subField == this.elementField;
        for (Iterator<?> i = this.getValueInternal(tx, id).iterator(); i.hasNext(); ) {
            final ObjId ref = (ObjId)i.next();
            if (ref != null && removedStorageIds.contains(ref.getStorageId()))
                i.remove();
        }
    }

    @Override
    boolean isUpgradeCompatible(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        final CollectionField<?, ?> that = (CollectionField<?, ?>)field;
        return this.elementField.isUpgradeCompatible(that.elementField);
    }
}
