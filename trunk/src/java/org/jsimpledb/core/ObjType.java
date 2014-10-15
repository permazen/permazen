
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

import org.jsimpledb.schema.SchemaField;
import org.jsimpledb.schema.SchemaObjectType;

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final FieldTypeRegistry fieldTypeRegistry;
    final TreeMap<Integer, Field<?>> fields = new TreeMap<>();
    final TreeMap<String, Field<?>> fieldsByName = new TreeMap<>();
    final TreeMap<Integer, SimpleField<?>> simpleFields = new TreeMap<>();
    final TreeMap<Integer, ComplexField<?>> complexFields = new TreeMap<>();
    final TreeMap<Integer, CounterField> counterFields = new TreeMap<>();
    final TreeMap<Integer, ReferenceField> referenceFields = new TreeMap<>();           // includes sub-fields too

    /**
     * Constructor.
     */
    ObjType(SchemaObjectType schemaObjectType, SchemaVersion version, FieldTypeRegistry fieldTypeRegistry) {
        super(schemaObjectType.getName(), schemaObjectType.getStorageId(), version);

        // Sanity check
        if (fieldTypeRegistry == null)
            throw new IllegalArgumentException("null fieldTypeRegistry");
        this.fieldTypeRegistry = fieldTypeRegistry;

        // Build fields
        final FieldBuilder fieldBuilder = new FieldBuilder(this.version, this.fieldTypeRegistry);
        for (SchemaField schemaField : schemaObjectType.getSchemaFields().values())
            this.addField(schemaField.visit(fieldBuilder));

        // Build mappings for various field types
        this.simpleFields.clear();
        this.rebuildMap(this.simpleFields, SimpleField.class);
        this.complexFields.clear();
        this.rebuildMap(this.complexFields, ComplexField.class);
        this.counterFields.clear();
        this.rebuildMap(this.counterFields, CounterField.class);
        for (ReferenceField referenceField : Iterables.filter(this.getFieldsAndSubFields(), ReferenceField.class))
            this.referenceFields.put(referenceField.storageId, referenceField);
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
     * Get the {@link Field} in this instance with the given storage ID.
     *
     * @param storageId storage ID
     * @return the {@link Field} with storage ID {@code storageID}
     * @throws UnknownFieldException if no {@link Field} with storage ID {@code storageId} exists
     */
    public Field<?> getField(int storageId) {
        final Field<?> field = this.fields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(this, storageId, "field");
        return field;
    }

    /**
     * Get all fields, including sub-fields.
     */
    Iterable<Field<?>> getFieldsAndSubFields() {
        Iterable<Field<?>> i = this.fields.values();
        for (ComplexField<?> field : this.complexFields.values())
            i = Iterables.concat(i, field.getSubFields());
        return i;
    }

    @Override
    ObjTypeStorageInfo toStorageInfo() {
        return new ObjTypeStorageInfo(this);
    }

    @Override
    public String toString() {
        return "object type `" + this.name + "' in " + this.version;
    }

// Internal methods

    private <T extends Field<?>> void rebuildMap(TreeMap<Integer, T> map, final Class<? super T> type) {
        map.putAll(Maps.transformValues(Maps.filterValues(this.fields, new Predicate<Field<?>>() {
            @Override
            public boolean apply(Field<?> field) {
                return type.isInstance(field);
            }
        }), new Function<Field<?>, T>() {
            @Override
            @SuppressWarnings("unchecked")
            public T apply(Field<?> field) {
                return (T)type.cast(field);
            }
        }));
    }

    private void addField(Field<?> field) {
        Field<?> previous = this.fields.put(field.storageId, field);
        if (previous != null) {
            throw new InconsistentDatabaseException("duplicate use of storage ID " + field.storageId
              + " by fields `" + previous.name + "' and `" + field.name + "' in " + this);
        }
        previous = this.fieldsByName.put(field.name, field);
        if (previous != null) {
            throw new InconsistentDatabaseException("duplicate use of name `" + field.storageId
              + "' by `" + previous + "' and `" + field + "' in " + this);
        }
    }
}

