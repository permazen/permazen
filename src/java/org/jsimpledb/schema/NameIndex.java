
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * An index of object, field, and index names in a {@link SchemaModel}.
 */
public class NameIndex {

    private final SchemaModel schemaModel;
    private final TreeMap<String, SchemaObjectType> typeMap = new TreeMap<>();
    private final TreeMap<Integer, TreeMap<String, SchemaField>> typeFieldMap = new TreeMap<>();
    private final TreeMap<Integer, TreeMap<String, SchemaCompositeIndex>> typeCompositeIndexMap = new TreeMap<>();

    /**
     * Constructor.
     *
     * @param schemaModel schema model to index
     * @throws IllegalArgumentException if a name conflict is detected (implies an invalid model)
     */
    public NameIndex(SchemaModel schemaModel) {

        // Initialize
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");
        this.schemaModel = schemaModel;

        // Index type names
        for (SchemaObjectType type : schemaModel.getSchemaObjectTypes().values()) {

            // Index type name
            if (this.typeMap.put(type.getName(), type) != null)
                throw new IllegalArgumentException("schema model is invalid");

            // Index field names
            final TreeMap<String, SchemaField> fieldMap = new TreeMap<>();
            this.typeFieldMap.put(type.getStorageId(), fieldMap);
            for (SchemaField field : type.getSchemaFields().values()) {
                if (fieldMap.put(field.getName(), field) != null)
                    throw new IllegalArgumentException("schema model is invalid");
            }

            // Index composite index names
            final TreeMap<String, SchemaCompositeIndex> compositeIndexMap = new TreeMap<>();
            this.typeCompositeIndexMap.put(type.getStorageId(), compositeIndexMap);
            for (SchemaCompositeIndex compositeIndex : type.getSchemaCompositeIndexes().values()) {
                if (compositeIndexMap.put(compositeIndex.getName(), compositeIndex) != null)
                    throw new IllegalArgumentException("schema model is invalid");
            }
        }
    }

    /**
     * Get the {@link SchemaModel} on which this index is based.
     *
     * @return associated schema
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get the {@link SchemaObjectType} with the given name.
     *
     * @param name type name
     * @throws IllegalArgumentException if {@code name} is null
     * @return the unique {@link SchemaObjectType} with name {@code name}, or null if not found
     */
    public SchemaObjectType getSchemaObjectType(String name) {
        Preconditions.checkArgument(name != null, "null name");
        return this.typeMap.get(name);
    }

    /**
     * Get the names of all {@link SchemaObjectType}s.
     *
     * @return unmodifiable set of {@link SchemaObjectType} names
     */
    public SortedSet<String> getSchemaObjectTypeNames() {
        return Collections.unmodifiableSortedSet(this.typeMap.navigableKeySet());
    }

    /**
     * Get the {@link SchemaField} with the given name in the given {@link SchemaObjectType}.
     *
     * @param type object type
     * @param name field name
     * @throws IllegalArgumentException if either paramter is null
     * @return the unique {@link SchemaField} with name {@code name} in {@code type}, or null if not found
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SchemaField getSchemaField(SchemaObjectType type, String name) {
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(name != null, "null name");
        final TreeMap<String, SchemaField> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return fieldMap.get(name);
    }

    /**
     * Get all of the names of {@link SchemaField}s in the given {@link SchemaObjectType}.
     *
     * @param type schema object
     * @return unmodifiable set of {@link SchemaField} names in {@code type}
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SortedSet<String> getSchemaFieldNames(SchemaObjectType type) {
        Preconditions.checkArgument(type != null, "null type");
        final TreeMap<String, SchemaField> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return Collections.unmodifiableSortedSet(fieldMap.navigableKeySet());
    }

    /**
     * Get the {@link SchemaCompositeIndex} with the given name in the given {@link SchemaObjectType}.
     *
     * @param type object type
     * @param name index name
     * @throws IllegalArgumentException if either paramter is null
     * @return the unique {@link SchemaCompositeIndex} with name {@code name} in {@code type}, or null if not found
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SchemaCompositeIndex getSchemaCompositeIndex(SchemaObjectType type, String name) {
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(name != null, "null name");
        final TreeMap<String, SchemaCompositeIndex> indexMap = this.typeCompositeIndexMap.get(type.getStorageId());
        if (indexMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return indexMap.get(name);
    }

    /**
     * Get all of the names of {@link SchemaCompositeIndex}s in the given {@link SchemaObjectType}.
     *
     * @param type schema object
     * @return unmodifiable set of {@link SchemaCompositeIndex} names in {@code type}
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SortedSet<String> getSchemaCompositeIndexNames(SchemaObjectType type) {
        Preconditions.checkArgument(type != null, "null type");
        final TreeMap<String, SchemaCompositeIndex> indexMap = this.typeCompositeIndexMap.get(type.getStorageId());
        if (indexMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return Collections.unmodifiableSortedSet(indexMap.navigableKeySet());
    }
}

