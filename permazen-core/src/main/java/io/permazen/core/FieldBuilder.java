
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.type.EnumValueEncoding;
import io.permazen.schema.CounterSchemaField;
import io.permazen.schema.EnumArraySchemaField;
import io.permazen.schema.EnumSchemaField;
import io.permazen.schema.ListSchemaField;
import io.permazen.schema.MapSchemaField;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.schema.SchemaFieldSwitch;
import io.permazen.schema.SetSchemaField;
import io.permazen.schema.SimpleSchemaField;

/**
 * Builds {@link Field}s from {@link SchemaField}s.
 */
class FieldBuilder implements SchemaFieldSwitch<Field<?>> {

    final Schema schema;
    final EncodingRegistry encodingRegistry;

    FieldBuilder(Schema schema, EncodingRegistry encodingRegistry) {
        Preconditions.checkArgument(schema != null, "null schema");
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");
        this.schema = schema;
        this.encodingRegistry = encodingRegistry;
    }

// SchemaFieldSwitch

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
        final EncodingId encodingId = field.getEncodingId();
        final Encoding<?> encoding = this.encodingRegistry.getEncoding(encodingId);
        if (encoding == null) {
            throw new IllegalArgumentException(
              String.format("unknown encoding \"%s\" for field \"%s\"", encodingId, field.getName()));
        }
        return this.buildSimpleField(field, field.getName(), encoding);
    }

    @Override
    public SimpleField<?> caseReferenceSchemaField(ReferenceSchemaField field) {
        return new ReferenceField(field.getName(), field.getStorageId(), this.schema, field.getOnDelete(),
          field.isCascadeDelete(), field.isAllowDeleted(), field.isAllowDeletedSnapshot(), field.getObjectTypes());
    }

    @Override
    public EnumField caseEnumSchemaField(EnumSchemaField field) {
        return new EnumField(field.getName(), field.getStorageId(), this.schema, field.isIndexed(), field.getIdentifiers());
    }

    @Override
    public EnumArrayField caseEnumArraySchemaField(EnumArraySchemaField field) {
        Preconditions.checkArgument(field.getDimensions() >= 1 && field.getDimensions() <= Encoding.MAX_ARRAY_DIMENSIONS);
        final EnumValueEncoding baseType = new EnumValueEncoding(field.getIdentifiers());
        Encoding<?> encoding = baseType;
        for (int dims = 0; dims < field.getDimensions(); dims++)
            encoding = SimpleEncodingRegistry.buildArrayEncoding(encoding);
        return new EnumArrayField(field.getName(), field.getStorageId(),
          this.schema, field.isIndexed(), baseType, encoding, field.getDimensions());
    }

    @Override
    public CounterField caseCounterSchemaField(CounterSchemaField field) {
        return new CounterField(field.getName(), field.getStorageId(), this.schema);
    }

// Internal methods

    // This method exists solely to bind the generic type parameters
    private <T> SimpleField<T> buildSimpleField(SimpleSchemaField field, String fieldName, Encoding<T> encoding) {
        return new SimpleField<>(fieldName, field.getStorageId(), this.schema, encoding, field.isIndexed());
    }

    // This method exists solely to bind the generic type parameters
    private <E> SetField<E> buildSetField(SetSchemaField field, SimpleField<E> elementField) {
        return new SetField<>(field.getName(), field.getStorageId(), this.schema, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <E> ListField<E> buildListField(ListSchemaField field, SimpleField<E> elementField) {
        return new ListField<>(field.getName(), field.getStorageId(), this.schema, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <K, V> MapField<K, V> buildMapField(MapSchemaField field, SimpleField<K> keyField, SimpleField<V> valueField) {
        return new MapField<>(field.getName(), field.getStorageId(), this.schema, keyField, valueField);
    }
}
