
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.schema.CounterSchemaField;
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
        return this.buildSetField(field, (SimpleField<?>)field.getElementField().visit(this));
    }

    @Override
    public ListField<?> caseListSchemaField(ListSchemaField field) {
        return this.buildListField(field, (SimpleField<?>)field.getElementField().visit(this));
    }

    @Override
    public MapField<?, ?> caseMapSchemaField(MapSchemaField field) {
        return this.buildMapField(field,
          (SimpleField<?>)field.getKeyField().visit(this), (SimpleField<?>)field.getValueField().visit(this));
    }

    @Override
    public SimpleField<?> caseSimpleSchemaField(SimpleSchemaField field) {
        final String fieldTypeName = field.getType();
        final FieldType<?> fieldType = this.fieldTypeRegistry.getFieldType(fieldTypeName);
        if (fieldType == null) {
            throw new IllegalArgumentException("unknown field type `" + fieldTypeName
              + "' for field `" + field.getName() + "' in " + this);
        }
        return this.buildSimpleField(field, field.getName(), fieldType);
    }

    @Override
    public SimpleField<?> caseReferenceSchemaField(ReferenceSchemaField field) {
        return new ReferenceField(field.getName(), field.getStorageId(), this.version, field.getOnDelete());
    }

    @Override
    public CounterField caseCounterSchemaField(CounterSchemaField field) {
        return new CounterField(field.getName(), field.getStorageId(), this.version);
    }

// Internal methods

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

