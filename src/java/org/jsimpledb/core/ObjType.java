
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
import org.jsimpledb.schema.SchemaObject;

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
        final FieldBuilder fieldBuilder = new FieldBuilder(this.version, this.fieldTypeRegistry);
        for (SchemaField schemaField : schemaObject.getSchemaFields().values())
            this.addField(schemaField.visit(fieldBuilder));

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
}

