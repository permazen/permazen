
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.TreeSet;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferenceFieldType;
import org.jsimpledb.schema.ReferenceSchemaField;

/**
 * Represents a reference field in a {@link JClass} or a reference sub-field of a complex field in a {@link JClass}.
 */
public class JReferenceField extends JSimpleField {

    final DeleteAction onDelete;
    final boolean cascadeDelete;

    JReferenceField(JSimpleDB jdb, String name, int storageId, String description, TypeToken<?> typeToken,
      org.jsimpledb.annotation.JField annotation, Method getter, Method setter) {
        super(jdb, name, storageId, typeToken, new ReferenceFieldType(), true, annotation, description, getter, setter);
        this.onDelete = annotation.onDelete();
        this.cascadeDelete = annotation.cascadeDelete();
    }

    @Override
    public JObject getValue(JObject jobj) {
        return (JObject)super.getValue(jobj);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJReferenceField(this);
    }

    @Override
    public Converter<ObjId, JObject> getConverter(JTransaction jtx) {
        return new ReferenceConverter<JObject>(jtx, JObject.class).reverse();
    }

    /**
     * Get the {@link DeleteAction} configured for this field.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     */
    public boolean isCascadeDelete() {
        return this.cascadeDelete;
    }

    @Override
    ReferenceSchemaField toSchemaItem(JSimpleDB jdb) {
        final ReferenceSchemaField schemaField = new ReferenceSchemaField();
        super.initialize(jdb, schemaField);
        schemaField.setOnDelete(this.onDelete);
        schemaField.setCascadeDelete(this.cascadeDelete);
        final TreeSet<Integer> objectTypes = new TreeSet<>();
        for (JClass<?> jclass : jdb.getJClasses(this.typeToken.getRawType()))
            objectTypes.add(jclass.storageId);
        schemaField.setObjectTypes(objectTypes);
        return schemaField;
    }

    @Override
    JReferenceFieldInfo toJFieldInfo(int parentStorageId) {
        return new JReferenceFieldInfo(this, parentStorageId);
    }
}

