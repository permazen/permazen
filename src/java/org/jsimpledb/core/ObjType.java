
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.schema.ListSchemaField;
import org.jsimpledb.schema.MapSchemaField;
import org.jsimpledb.schema.ReferenceSchemaField;
import org.jsimpledb.schema.SchemaField;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.schema.SetSchemaField;
import org.jsimpledb.schema.SimpleSchemaField;

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final FieldTypeRegistry fieldTypeRegistry;
    final TreeMap<Integer, Field<?>> fields = new TreeMap<>();
    final TreeMap<Integer, SimpleField<?>> simpleFields = new TreeMap<>();
    final TreeMap<Integer, ComplexField<?>> complexFields = new TreeMap<>();

    /**
     * Constructor.
     */
    ObjType(SchemaObject schemaObject, SchemaVersion version, FieldTypeRegistry fieldTypeRegistry) {
        super(schemaObject.getName(), schemaObject.getStorageId(), version);

        // Sanity check
        if (fieldTypeRegistry == null)
            throw new IllegalArgumentException("null fieldTypeRegistry");
        this.fieldTypeRegistry = fieldTypeRegistry;

        // Build fields
        for (SchemaField schemaField : schemaObject.getSchemaFields().values()) {
            if (schemaField instanceof SimpleSchemaField)
                this.addField(this.buildSimpleField((SimpleSchemaField)schemaField, schemaField.getName()));
            else if (schemaField instanceof SetSchemaField) {
                final SetSchemaField setField = (SetSchemaField)schemaField;
                this.addField(this.buildSetField(setField,
                  this.buildSimpleField(setField.getElementField(), SetField.ELEMENT_FIELD_NAME)));
            } else if (schemaField instanceof ListSchemaField) {
                final ListSchemaField listField = (ListSchemaField)schemaField;
                this.addField(this.buildListField(listField,
                  this.buildSimpleField(listField.getElementField(), ListField.ELEMENT_FIELD_NAME)));
            } else if (schemaField instanceof MapSchemaField) {
                final MapSchemaField mapField = (MapSchemaField)schemaField;
                this.addField(this.buildMapField(mapField,
                  this.buildSimpleField(mapField.getKeyField(), MapField.KEY_FIELD_NAME),
                  this.buildSimpleField(mapField.getValueField(), MapField.VALUE_FIELD_NAME)));
            } else
                throw new RuntimeException("internal error");
        }

        // Build mappings for only simple and only complex fields
        this.simpleFields.clear();
        this.simpleFields.putAll(Maps.transformValues(Maps.filterValues(this.fields, new Predicate<Field<?>>() {
            @Override
            public boolean apply(Field<?> field) {
                return field instanceof SimpleField;
            }
        }), new Function<Field<?>, SimpleField<?>>() {
            @Override
            public SimpleField<?> apply(Field<?> field) {
                return (SimpleField<?>)field;
            }
        }));
        this.complexFields.clear();
        this.complexFields.putAll(Maps.transformValues(Maps.filterValues(this.fields, new Predicate<Field<?>>() {
            @Override
            public boolean apply(Field<?> field) {
                return field instanceof ComplexField;
            }
        }), new Function<Field<?>, ComplexField<?>>() {
            @Override
            public ComplexField<?> apply(Field<?> field) {
                return (ComplexField<?>)field;
            }
        }));
    }

    /**
     * Get all fields associated with this object type. Does not include sub-fields of complex fields.
     *
     * @return unmodifiable mapping from {@linkplain Field#getStorageId field storage ID} to field
     */
    public SortedMap<Integer, Field<?>> getFields() {
        return Collections.unmodifiableSortedMap(this.fields);
    }

    /**
     * Get all fields, including sub-fields.
     */
    Iterable<Field<?>> getFieldsAndSubFields() {
        Iterable<Field<?>> i = Iterables.concat(this.simpleFields.values(), this.complexFields.values());
        for (ComplexField<?> field : this.complexFields.values())
            i = Iterables.concat(i, field.getSubFields());
        return i;
    }

    ObjTypeStorageInfo toStorageInfo() {
        return new ObjTypeStorageInfo(this);
    }

    @Override
    public String toString() {
        return "object type `" + this.name + "' in " + this.version;
    }

// Internal methods

    private void addField(Field<?> field) {
        final Field<?> previous = this.fields.put(field.storageId, field);
        if (previous != null) {
            throw new InconsistentDatabaseException("duplicate use of storage ID " + field.storageId
              + " by fields `" + previous.name + "' and `" + field.name + "' in " + this);
        }
    }

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

