
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.TreeSet;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.schema.ReferenceSchemaField;

/**
 * Represents a reference field in a {@link JClass} or a reference sub-field of a complex field in a {@link JClass}.
 */
public class JReferenceField extends JSimpleField {

    final DeleteAction onDelete;

    JReferenceField(String name, int storageId, String description,
      TypeToken<?> typeToken, DeleteAction onDelete, Method getter, Method setter) {
        super(name, storageId, typeToken, FieldType.REFERENCE_TYPE_NAME, true, description, getter, setter);
        this.onDelete = onDelete;
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJReferenceField(this);
    }

    /**
     * Get the {@link DeleteAction} configured for this field.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    @Override
    ReferenceSchemaField toSchemaItem(JSimpleDB jdb) {
        final ReferenceSchemaField schemaField = new ReferenceSchemaField();
        super.initialize(jdb, schemaField);
        schemaField.setOnDelete(this.onDelete);
        final TreeSet<Integer> objectTypes = new TreeSet<>();
        for (JClass<?> jclass : jdb.getJClasses(this.typeToken))
            objectTypes.add(jclass.storageId);
        schemaField.setObjectTypes(objectTypes);
        return schemaField;
    }

    @Override
    JReferenceFieldInfo toJFieldInfo(JComplexFieldInfo parent) {
        return new JReferenceFieldInfo(this, parent);
    }
}

