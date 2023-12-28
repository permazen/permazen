
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.schema.CollectionSchemaField;
import io.permazen.schema.SimpleSchemaField;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Superclass of fields with types assignable to {@link Collection}.
 *
 * @param <C> Java collection type
 * @param <E> Java collection element type
 */
public abstract class CollectionField<C extends Collection<E>, E> extends ComplexField<C> {

    public static final String ELEMENT_FIELD_NAME = "element";

    final SimpleField<E> elementField;

    CollectionField(ObjType objType, CollectionSchemaField field, TypeToken<C> typeToken, SimpleField<E> elementField) {
        super(objType, field, typeToken);
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

    /**
     * Get the index on this fields's {@value #ELEMENT_FIELD_NAME} sub-field.
     *
     * @return the index on this field's {@value #ELEMENT_FIELD_NAME} sub-field
     * @throws UnknownIndexException if there is no index on the {@value #ELEMENT_FIELD_NAME} sub-field
     */
    @SuppressWarnings("unchecked")
    public CollectionElementIndex<C, E> getElementIndex() {
        return (CollectionElementIndex<C, E>)this.elementField.getIndex();
    }

    @Override
    public final List<SimpleField<E>> getSubFields() {
        return Collections.singletonList(this.elementField);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id).isEmpty();
    }

    @Override
    public abstract C getValue(Transaction tx, ObjId id);

// Package methods

    @Override
    @SuppressWarnings("unchecked")
    final CollectionElementIndex<C, E> createSubFieldIndex(
      Schema schema, SimpleSchemaField schemaField, ObjType objType, SimpleField<?> field) {
        Preconditions.checkArgument(field == this.getElementField(), "wrong sub-field");
        return this.createElementSubFieldIndex(schema, schemaField, objType);
    }

    abstract CollectionElementIndex<C, E> createElementSubFieldIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType);

    @Override
    @SuppressWarnings("unchecked")
    <F> Iterable<F> iterateSubField(Transaction tx, ObjId id, SimpleField<F> subField) {
        if (subField == this.elementField)
            return (Iterable<F>)this.getValue(tx, id);
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    void unreferenceRemovedTypes(Transaction tx, ObjId id, ReferenceField subField, Set<Integer> removedStorageIds) {
        assert subField == this.elementField;
        for (Iterator<?> i = this.getValueInternal(tx, id).iterator(); i.hasNext(); ) {
            final ObjId ref = (ObjId)i.next();
            if (ref != null && removedStorageIds.contains(ref.getStorageId()))
                i.remove();
        }
    }
}
