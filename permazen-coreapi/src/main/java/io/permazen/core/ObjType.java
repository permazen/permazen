
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.schema.SchemaField;
import io.permazen.schema.SchemaObjectType;

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final FieldTypeRegistry fieldTypeRegistry;
    final TreeMap<Integer, Field<?>> fields = new TreeMap<>();                              // does not include sub-fields
    final TreeMap<String, Field<?>> fieldsByName = new TreeMap<>();
    final ArrayList<Field<?>> fieldsAndSubFields = new ArrayList<>();                       // includes sub-fields
    final TreeMap<Integer, CompositeIndex> compositeIndexes = new TreeMap<>();
    final TreeMap<String, CompositeIndex> compositeIndexesByName = new TreeMap<>();
    final TreeMap<Integer, SimpleField<?>> simpleFields = new TreeMap<>();
    final ArrayList<SimpleField<?>> indexedSimpleFields = new ArrayList<>();
    final TreeMap<Integer, ComplexField<?>> complexFields = new TreeMap<>();
    final TreeMap<Integer, CounterField> counterFields = new TreeMap<>();
    final TreeMap<Integer, ReferenceField> referenceFieldsAndSubFields = new TreeMap<>();   // includes sub-fields

    /**
     * Constructor.
     */
    ObjType(SchemaObjectType schemaObjectType, Schema schema, FieldTypeRegistry fieldTypeRegistry) {
        super(schemaObjectType.getName(), schemaObjectType.getStorageId(), schema);

        // Sanity check
        Preconditions.checkArgument(fieldTypeRegistry != null, "null fieldTypeRegistry");
        this.fieldTypeRegistry = fieldTypeRegistry;

        // Build fields
        final FieldBuilder fieldBuilder = new FieldBuilder(this.schema, this.fieldTypeRegistry);
        for (SchemaField schemaField : schemaObjectType.getSchemaFields().values())
            this.addSchemaItem(fields, fieldsByName, schemaField.visit(fieldBuilder));

        // Build mappings for various field types
        this.buildMap(this.simpleFields, SimpleField.class);
        this.buildMap(this.complexFields, ComplexField.class);
        this.buildMap(this.counterFields, CounterField.class);
        for (SimpleField<?> simpleField : this.simpleFields.values()) {
            if (simpleField.indexed)
                this.indexedSimpleFields.add(simpleField);
        }
        this.indexedSimpleFields.trimToSize();

        // Build list of all fields including sub-fields
        this.fieldsAndSubFields.addAll(this.fields.values());
        for (ComplexField<?> field : this.complexFields.values())
            this.fieldsAndSubFields.addAll(field.getSubFields());

        // Build mappings for reference fields
        this.fieldsAndSubFields.stream()
          .filter(field -> field instanceof ReferenceField)
          .forEach(field -> this.referenceFieldsAndSubFields.put(field.storageId, (ReferenceField)field));

        // Build composite indexes
        for (SchemaCompositeIndex schemaIndex : schemaObjectType.getSchemaCompositeIndexes().values())
            this.addCompositeIndex(this.schema, schemaIndex);

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
     * <p>
     * This version does not find sub-fields of {@link ComplexField}s; use {@link #getField(int, boolean)} for that.
     *
     * @param storageId storage ID
     * @return the {@link Field} with storage ID {@code storageID}
     * @throws UnknownFieldException if no top-level {@link Field} with storage ID {@code storageId} exists
     * @see #getField(int, boolean)
     */
    public Field<?> getField(int storageId) {
        return this.getField(storageId, false);
    }

    /**
     * Get the {@link Field} in this instance with the given storage ID, optionally searching sub-fields.
     *
     * @param storageId storage ID
     * @param searchSubFields whether to search in sub-fields as well
     * @return the {@link Field} with storage ID {@code storageID}
     * @throws UnknownFieldException if no {@link Field} with storage ID {@code storageId} exists
     */
    public Field<?> getField(int storageId, boolean searchSubFields) {
        final Field<?> field = this.fields.get(storageId);
        if (field != null)
            return field;
        if (searchSubFields) {
            for (Field<?> parent : this.fields.values()) {
                if (!(parent instanceof ComplexField))
                    continue;
                for (SimpleField<?> subField : ((ComplexField<?>)parent).getSubFields()) {
                    if (subField.storageId == storageId)
                        return subField;
                }
            }
        }
        throw new UnknownFieldException(this, storageId, "field");
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
            throw new UnknownIndexException(storageId, "no composite index with storage ID " + storageId + " exists");
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

    @Override
    ObjTypeStorageInfo toStorageInfo() {
        return new ObjTypeStorageInfo(this);
    }

    @Override
    public String toString() {
        return "object type `" + this.name + "' in " + this.schema;
    }

// Internal methods

    @SuppressWarnings("unchecked")
    private <T extends Field<?>> void buildMap(TreeMap<Integer, T> map, final Class<? super T> type) {
        this.fields.values().stream()
          .filter(field -> type.isInstance(field))
          .forEach(field -> map.put(field.storageId, (T)type.cast(field)));
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

    private CompositeIndex addCompositeIndex(Schema schema, SchemaCompositeIndex schemaIndex) {

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
        final CompositeIndex index = new CompositeIndex(schemaIndex.getName(), schemaIndex.getStorageId(), schema, this, list);
        this.addSchemaItem(this.compositeIndexes, this.compositeIndexesByName, index);
        return index;
    }
}

