
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.List;

import org.jsimpledb.schema.ComplexSchemaField;

/**
 * Represents a complex field in a {@link JClass}.
 */
public abstract class JComplexField extends JField {

    JComplexField(JSimpleDB jdb, String name, int storageId, String description, Method getter) {
        super(jdb, name, storageId, description, getter);
        if (name == null)
            throw new IllegalArgumentException("null name");
    }

    @Override
    abstract ComplexSchemaField toSchemaItem(JSimpleDB jdb);

    /**
     * Get the sub-fields associated with this field.
     *
     * @return this instance's sub-fields (in order)
     */
    public abstract List<JSimpleField> getSubFields();

    /**
     * Get the sub-field with the given name.
     *
     * @param name sub-field name
     * @return the sub-field with the specified name
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    public abstract JSimpleField getSubField(String name);

    /**
     * Get the sub-field with the given storage ID.
     *
     * @param storageId sub-field storage ID
     * @throws IllegalArgumentException if not found
     */
    JSimpleField getSubField(int storageId) {
        for (JSimpleField subField : this.getSubFields()) {
            if (subField.storageId == storageId)
                return subField;
        }
        throw new IllegalArgumentException("storage ID " + storageId + " not found");
    }

    /**
     * Get the name of the given sub-field.
     *
     * @throws IllegalArgumentException if {@code subField} is not one of {@link #getSubFields}
     */
    abstract String getSubFieldName(JSimpleField subField);
}

