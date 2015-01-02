
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
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.schema.SchemaCompositeIndex;
import org.jsimpledb.schema.SchemaField;
import org.jsimpledb.schema.SchemaObjectType;

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final FieldTypeRegistry fieldTypeRegistry;
    final TreeMap<Integer, Field<?>> fields = new TreeMap<>();
    final TreeMap<String, Field<?>> fieldsByName = new TreeMap<>();
    final TreeMap<Integer, CompositeIndex> compositeIndexes = new TreeMap<>();
    final TreeMap<String, CompositeIndex> compositeIndexesByName = new TreeMap<>();
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
            this.addSchemaItem(fields, fieldsByName, schemaField.visit(fieldBuilder));

        // Build mappings for various field types
        this.buildMap(this.simpleFields, SimpleField.class);
        this.buildMap(this.complexFields, ComplexField.class);
        this.buildMap(this.counterFields, CounterField.class);
        for (ReferenceField referenceField : Iterables.filter(this.getFieldsAndSubFields(), ReferenceField.class))
            this.referenceFields.put(referenceField.storageId, referenceField);

        // Build composite indexes
        for (SchemaCompositeIndex schemaIndex : schemaObjectType.getSchemaCompositeIndexes().values())
            this.addCompositeIndex(this.version, schemaIndex);

        // Link simple fields to the composite indexes they include
        for (SimpleField<?> field : this.simpleFields.values()) {
            final HashMap<CompositeIndex, Integer> indexMap = new HashMap<>();
            for (CompositeIndex index : this.compositeIndexes.values()) {
                final int i = index.fields.indexOf(field);
                if (i != -1)
                    indexMap.put(index, i);
            }
            assert field.compositeIndexMap == null;
            if (!indexMap.isEmpty())
                field.compositeIndexMap = Collections.unmodifiableMap(indexMap);
        }
    }

    /**
     * Get all fields associated with this object type keyed by storage ID. Does not include sub-fields of complex fields.
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
     * Get all fields associated with this object type keyed by name. Does not include sub-fields of complex fields.
     *
     * @return unmodifiable mapping from {@linkplain Field#getName field name} to field
     */
    public SortedMap<String, Field<?>> getFieldsByName() {
        return Collections.unmodifiableSortedMap(this.fieldsByName);
    }

    /**
     * Get all composite indexes associated with this object type keyed by storage ID.
     *
     * @return unmodifiable mapping from {@linkplain CompositeIndex#getStorageId composite index storage ID} to field
     */
    public SortedMap<Integer, CompositeIndex> getCompositeIndexes() {
        return Collections.unmodifiableSortedMap(this.compositeIndexes);
    }

    /**
     * Get the {@link CompositeIndex} associated with this instance with the given storage ID.
     *
     * @param storageId storage ID
     * @return the {@link CompositeIndex} with storage ID {@code storageID}
     * @throws UnknownIndexException if no {@link CompositeIndex} with storage ID {@code storageId} exists
     */
    public CompositeIndex getCompositeIndex(int storageId) {
        final CompositeIndex index = this.compositeIndexes.get(storageId);
        if (index == null)
            throw new UnknownIndexException(storageId, "composite index");
        return index;
    }

    /**
     * Get all composite indexes associated with this object type keyed by name.
     *
     * @return unmodifiable mapping from {@linkplain CompositeIndex#getName index name} to index
     */
    public SortedMap<String, CompositeIndex> getCompositeIndexesByName() {
        return Collections.unmodifiableSortedMap(this.compositeIndexesByName);
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

    private <T extends Field<?>> void buildMap(TreeMap<Integer, T> map, final Class<? super T> type) {
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

    private <T extends SchemaItem> void addSchemaItem(Map<Integer, T> byStorageId, Map<String, T> byName, T item) {
        T previous = byStorageId.put(item.storageId, item);
        if (previous != null) {
            throw new IllegalArgumentException("duplicate use of storage ID " + item.storageId
              + " by " + previous + " and " + item + " in " + this);
        }
        previous = byName.put(item.name, item);
        if (previous != null) {
            throw new IllegalArgumentException("duplicate use of name `" + item.name
              + "' by " + previous + " and " + item + " in " + this);
        }
    }

    private CompositeIndex addCompositeIndex(SchemaVersion version, SchemaCompositeIndex schemaIndex) {

        // Get fields corresponding to specified storage IDs
        final int[] storageIds = Ints.toArray(schemaIndex.getIndexedFields());
        if (storageIds.length < 2 || storageIds.length > Database.MAX_INDEXED_FIELDS)
            throw new IllegalArgumentException("invalid " + schemaIndex + ": can't index " + storageIds.length + " fields");
        final ArrayList<SimpleField<?>> list = new ArrayList<>(storageIds.length);
        int count = 0;
        for (int storageId : storageIds) {
            final Field<?> field = this.fields.get(storageId);
            if (!(field instanceof SimpleField)) {
                throw new IllegalArgumentException("invalid " + schemaIndex
                  + ": no simple field with storage ID " + storageId + " found");
            }
            final SimpleField<?> simpleField = (SimpleField<?>)field;
            if (simpleField.parent != null) {
                throw new IllegalArgumentException("invalid " + schemaIndex
                  + ": simple field with storage ID " + storageId + " is a sub-field of a complex field");
            }
            list.add(simpleField);
        }

        // Create and add index
        final CompositeIndex index = new CompositeIndex(schemaIndex.getName(), schemaIndex.getStorageId(), version, this, list);
        this.addSchemaItem(this.compositeIndexes, this.compositeIndexesByName, index);
        return index;
    }
}

