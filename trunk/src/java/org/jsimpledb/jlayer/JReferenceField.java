
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

import org.jsimpledb.DeleteAction;
import org.jsimpledb.ReferenceType;
import org.jsimpledb.schema.ReferenceSchemaField;

/**
 * Represents a reference field in a {@link JClass} or a reference sub-field of a complex field in a {@link JClass}.
 */
public class JReferenceField extends JSimpleField {

    final DeleteAction onDelete;

    JReferenceField(String name, int storageId, String description,
      TypeToken<?> typeToken, DeleteAction onDelete, Method getter, Method setter) {
        super(name, storageId, typeToken, ReferenceType.NAME, true, description, getter, setter);
        this.onDelete = onDelete;
    }

    /**
     * Get the {@link DeleteAction} configured for this field.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    @Override
    ReferenceSchemaField toSchemaItem() {
        final ReferenceSchemaField schemaField = new ReferenceSchemaField();
        super.initialize(schemaField);
        schemaField.setOnDelete(this.onDelete);
        return schemaField;
    }
}

