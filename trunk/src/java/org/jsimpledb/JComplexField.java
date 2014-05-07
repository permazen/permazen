
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Deque;
import java.util.List;
import java.util.Set;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.ComplexSchemaField;

/**
 * Represents a complex field in a {@link JClass}.
 */
public abstract class JComplexField extends JField {

    JComplexField(String name, int storageId, String description, Method getter) {
        super(name, storageId, description, getter);
        if (name == null)
            throw new IllegalArgumentException("null name");
    }

    @Override
    abstract ComplexSchemaField toSchemaItem();

    /**
     * Get the sub-fields associated with this field.
     */
    public abstract List<JSimpleField> getSubFields();

    /**
     * Get the sub-field with the given name.
     *
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    public abstract JSimpleField getSubField(String name);

    /**
     * Get the name of the given sub-field.
     *
     * @throws IllegalArgumentException if {@code subField} is not one of {@link #getSubFields}
     */
    abstract String getSubFieldName(JSimpleField subField);

    /**
     * Add any valid index entry return types for @IndexQuery-annotated methods that query the given indexed sub-field.
     */
    abstract <T> void addIndexEntryReturnTypes(List<TypeToken<?>> types, TypeToken<T> targetType, JSimpleField subField);

    /**
     * Determine the type of index query from the method return type, i.e., normal object query or some kind of index entry query.
     *
     * @return zero for a normal query or some field-specific value otherwise
     */
    abstract int getIndexEntryQueryType(TypeToken<?> queryObjectType);

    /**
     * Get the {@link JTransaction} method to invoke from generated classes for the given index entry query type.
     */
    abstract Method getIndexEntryQueryMethod(int queryType);

    /**
     * Recurse for copying between transactions.
     */
    abstract void copyRecurse(Set<ObjId> seen, JTransaction src, JTransaction dest,
      ObjId id, JReferenceField subField, Deque<JReferenceField> nextFields);

    void copyRecurse(Set<ObjId> seen, JTransaction src, JTransaction dest, Iterable<?> it, Deque<JReferenceField> nextFields) {
        for (Object obj : it) {
            if (obj != null)
                src.copyTo(seen, dest, (ObjId)obj, nextFields);
        }
    }
}

