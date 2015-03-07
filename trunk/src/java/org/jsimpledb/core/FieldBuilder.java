
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.schema.CounterSchemaField;
import org.jsimpledb.schema.EnumSchemaField;
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

    final Schema schema;
    final FieldTypeRegistry fieldTypeRegistry;

    FieldBuilder(Schema schema, FieldTypeRegistry fieldTypeRegistry) {
        if (schema == null)
            throw new IllegalArgumentException("null schema");
        if (fieldTypeRegistry == null)
            throw new IllegalArgumentException("null fieldTypeRegistry");
        this.schema = schema;
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
        if (field.getEncodingSignature() != 0)
            throw new IllegalArgumentException("encoding signature must be zero for " + field);
        return new ReferenceField(field.getName(), field.getStorageId(),
          this.schema, field.getOnDelete(), field.isCascadeDelete(), field.getObjectTypes());
    }

    @Override
    public EnumField caseEnumSchemaField(EnumSchemaField field) {
        if (field.getEncodingSignature() != 0)
            throw new IllegalArgumentException("encoding signature must be zero for " + field);
        return new EnumField(field.getName(), field.getStorageId(), this.schema,
          field.isIndexed(), field.getType(), field.getIdentifiers());
    }

    @Override
    public CounterField caseCounterSchemaField(CounterSchemaField field) {
        return new CounterField(field.getName(), field.getStorageId(), this.schema);
    }

// Internal methods

    // This method exists solely to bind the generic type parameters
    private <T> SimpleField<T> buildSimpleField(SimpleSchemaField field, String fieldName, FieldType<T> fieldType) {
        if (field.getEncodingSignature() != fieldType.getEncodingSignature()) {
            throw new IllegalArgumentException("incompatible encoding signatures: field type `" + fieldType.getName()
              + "' has " + fieldType.getEncodingSignature() + " but schema is using " + field.getEncodingSignature());
        }
        return new SimpleField<T>(fieldName, field.getStorageId(), this.schema, fieldType, field.isIndexed());
    }

    // This method exists solely to bind the generic type parameters
    private <E> SetField<E> buildSetField(SetSchemaField field, SimpleField<E> elementField) {
        return new SetField<E>(field.getName(), field.getStorageId(), this.schema, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <E> ListField<E> buildListField(ListSchemaField field, SimpleField<E> elementField) {
        return new ListField<E>(field.getName(), field.getStorageId(), this.schema, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <K, V> MapField<K, V> buildMapField(MapSchemaField field, SimpleField<K> keyField, SimpleField<V> valueField) {
        return new MapField<K, V>(field.getName(), field.getStorageId(), this.schema, keyField, valueField);
    }
}

