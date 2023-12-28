
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.encoding.SimpleEncodingRegistry;
import io.permazen.schema.CounterSchemaField;
import io.permazen.schema.EnumArraySchemaField;
import io.permazen.schema.EnumSchemaField;
import io.permazen.schema.ListSchemaField;
import io.permazen.schema.MapSchemaField;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.schema.SchemaField;
import io.permazen.schema.SchemaFieldSwitch;
import io.permazen.schema.SetSchemaField;
import io.permazen.schema.SimpleSchemaField;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Builds {@link Field}s from {@link SchemaField}s.
 */
class FieldBuilder implements SchemaFieldSwitch<Field<?>> {

    final Schema schema;
    final ObjType objType;
    final EncodingRegistry encodingRegistry;
    final HashMap<SimpleField<?>, SimpleSchemaField> simpleModels = new HashMap<>();

    FieldBuilder(ObjType objType, EncodingRegistry encodingRegistry) {
        Preconditions.checkArgument(objType != null, "null objType");
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");
        this.schema = objType.getSchema();
        this.objType = objType;
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
          (SimpleField<?>)field.getKeyField().visit(this),
          (SimpleField<?>)field.getValueField().visit(this));
    }

    @Override
    public SimpleField<?> caseSimpleSchemaField(SimpleSchemaField field) {

        // Get encoding
        final EncodingId encodingId = field.getEncodingId();
        final Encoding<?> encoding = this.encodingRegistry.getEncoding(encodingId);
        if (encoding == null) {
            throw new InvalidSchemaException(
              String.format("unknown encoding \"%s\" for field \"%s\" in type \"%s\"",
              encodingId, field.getName(), this.objType.getName()));
        }

        // Build field
        return this.buildSimpleField(field, encoding, field.isIndexed());
    }

    @Override
    public SimpleField<?> caseReferenceSchemaField(ReferenceSchemaField field) {
        HashSet<ObjType> objTypes = null;
        if (field.getObjectTypes() != null) {
            objTypes = new HashSet<>();
            for (String typeName : field.getObjectTypes())
                objTypes.add(this.schema.getObjType(typeName));
        }
        return this.add(field, new ReferenceField(this.objType, field, objTypes));
    }

    @Override
    public EnumField caseEnumSchemaField(EnumSchemaField field) {
        return this.add(field, new EnumField(this.objType, field, field.isIndexed()));
    }

    @Override
    public EnumArrayField caseEnumArraySchemaField(EnumArraySchemaField field) {
        Preconditions.checkArgument(field.getDimensions() >= 1 && field.getDimensions() <= Encoding.MAX_ARRAY_DIMENSIONS);
        final EnumValueEncoding baseType = new EnumValueEncoding(field.getIdentifiers());
        Encoding<?> encoding = baseType;
        for (int dims = 0; dims < field.getDimensions(); dims++)
            encoding = SimpleEncodingRegistry.buildArrayEncoding(encoding);
        return this.add(field, new EnumArrayField(this.objType, field, baseType, encoding, field.isIndexed()));
    }

    @Override
    public CounterField caseCounterSchemaField(CounterSchemaField field) {
        return new CounterField(this.objType, field);
    }

// Internal methods

    // This method exists solely to bind the generic type parameters
    private <T> SimpleField<T> buildSimpleField(SimpleSchemaField field, Encoding<T> encoding, boolean indexed) {
        return this.add(field, new SimpleField<>(this.objType, field, encoding, indexed));
    }

    // This method exists solely to bind the generic type parameters
    private <E> SetField<E> buildSetField(SetSchemaField field, SimpleField<E> elementField) {
        return new SetField<>(this.objType, field, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <E> ListField<E> buildListField(ListSchemaField field, SimpleField<E> elementField) {
        return new ListField<>(this.objType, field, elementField);
    }

    // This method exists solely to bind the generic type parameters
    private <K, V> MapField<K, V> buildMapField(MapSchemaField field, SimpleField<K> keyField, SimpleField<V> valueField) {
        return new MapField<>(this.objType, field, keyField, valueField);
    }

    // Maintain a map from built field to original schema item
    private <S extends SimpleSchemaField, F extends SimpleField<?>> F add(S schemaField, F field) {
        this.simpleModels.put(field, schemaField);
        return field;
    }
}
