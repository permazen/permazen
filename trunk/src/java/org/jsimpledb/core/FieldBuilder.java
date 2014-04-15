
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.schema.ListSchemaField;
import org.jsimpledb.schema.MapSchemaField;
import org.jsimpledb.schema.ReferenceSchemaField;
import org.jsimpledb.schema.SchemaFieldSwitchAdapter;
import org.jsimpledb.schema.SetSchemaField;
import org.jsimpledb.schema.SimpleSchemaField;

/**
 * Builds {@link Field}s from {@link SchemaField}s.
 */
class FieldBuilder extends SchemaFieldSwitchAdapter<Field<?>> {

    final SchemaVersion version;
    final FieldTypeRegistry fieldTypeRegistry;

    FieldBuilder(SchemaVersion version, FieldTypeRegistry fieldTypeRegistry) {
        if (version == null)
            throw new IllegalArgumentException("null version");
        if (fieldTypeRegistry == null)
            throw new IllegalArgumentException("null fieldTypeRegistry");
        this.version = version;
        this.fieldTypeRegistry = fieldTypeRegistry;
    }

// SchemaFieldSwitchAdapter

    @Override
    public SetField<?> caseSetSchemaField(SetSchemaField field) {
        return this.buildSetField(field, this.buildSimpleField(field.getElementField(), SetField.ELEMENT_FIELD_NAME));
    }

    @Override
    public ListField<?> caseListSchemaField(ListSchemaField field) {
        return this.buildListField(field, this.buildSimpleField(field.getElementField(), ListField.ELEMENT_FIELD_NAME));
    }

    @Override
    public MapField<?, ?> caseMapSchemaField(MapSchemaField field) {
        return this.buildMapField(field,
          this.buildSimpleField(field.getKeyField(), MapField.KEY_FIELD_NAME),
          this.buildSimpleField(field.getValueField(), MapField.VALUE_FIELD_NAME));
    }

    @Override
    public SimpleField<?> caseSimpleSchemaField(SimpleSchemaField field) {
        return this.buildSimpleField(field, field.getName());
    }

// Internal methods

    private SimpleField<?> buildSimpleField(SimpleSchemaField simpleField, String fieldName) {
        if (simpleField instanceof ReferenceSchemaField) {
            final ReferenceSchemaField refField = (ReferenceSchemaField)simpleField;
            return new ReferenceField(fieldName, refField.getStorageId(), this.version, refField.getOnDelete());
        }
        final String fieldTypeName = simpleField.getType();
        final FieldType<?> fieldType = this.fieldTypeRegistry.getFieldType(fieldTypeName);
        if (fieldType == null) {
            throw new IllegalArgumentException("unknown field type `" + fieldTypeName
              + "' for field `" + fieldName + "' in " + this);
        }
        return this.buildSimpleField(simpleField, fieldName, fieldType);
    }

    // This method exists solely to bind the generic type parameters
    private <T> SimpleField<T> buildSimpleField(SimpleSchemaField field, String fieldName, FieldType<T> fieldType) {
        return new SimpleField<T>(fieldName, field.getStorageId(), this.version, fieldType, field.isIndexed());
    }

    // This method exists solely to bind the generic type parameters
    private <E> SetField<E> buildSetField(SetSchemaField field, SimpleField<E> elementField) {
        return new SetField<E>(field.getName(), field.getStorageId(), this.version, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <E> ListField<E> buildListField(ListSchemaField field, SimpleField<E> elementField) {
        return new ListField<E>(field.getName(), field.getStorageId(), this.version, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <K, V> MapField<K, V> buildMapField(MapSchemaField field, SimpleField<K> keyField, SimpleField<V> valueField) {
        return new MapField<K, V>(field.getName(), field.getStorageId(), this.version, keyField, valueField);
    }
}

