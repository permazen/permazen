
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * An index of object and field names in a {@link SchemaModel}.
 *
 * <p>
 * Note that {@link SchemaModel} names are not required to be unique.
 * </p>
 */
public class NameIndex {

    private final SchemaModel schemaModel;
    private final TreeMap<String, SchemaObject> typeMap = new TreeMap<>();
    private final TreeMap<Integer, TreeMap<String, SchemaField>> typeFieldMap = new TreeMap<>();

    /**
     * Constructor.
     *
     * @param schemaModel schema model to index
     */
    public NameIndex(SchemaModel schemaModel) {

        // Initialize
        if (schemaModel == null)
            throw new IllegalArgumentException("null schemaModel");
        this.schemaModel = schemaModel;

        // Index type names
        for (SchemaObject type : schemaModel.getSchemaObjects().values()) {

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
        }
    }

    /**
     * Get the {@link SchemaModel} on which this index is based.
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get the {@link SchemaObject} with the given name.
     *
     * @param name type name
     * @throws IllegalArgumentException if {@code name} is null
     * @return the unique {@link SchemaObject} with name {@code name}, or null if not found
     */
    public SchemaObject getSchemaObject(String name) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        return this.typeMap.get(name);
    }

    /**
     * Get the names of all {@link SchemaObject}s.
     *
     * @return unmodifiable set of {@link SchemaObject} names
     */
    public SortedSet<String> getSchemaObjectNames() {
        return Collections.unmodifiableSortedSet(this.typeMap.navigableKeySet());
    }

    /**
     * Get the {@link SchemaField} with the given name in the given {@link SchemaObject}.
     *
     * @param type object type
     * @param name field name
     * @throws IllegalArgumentException if either paramter is null
     * @return the unique {@link SchemaField} with name {@code name} in {@code type}, or null if not found
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SchemaField getSchemaField(SchemaObject type, String name) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (name == null)
            throw new IllegalArgumentException("null name");
        final TreeMap<String, SchemaField> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return fieldMap.get(name);
    }

    /**
     * Get all of the names of {@link SchemaField}s in the given {@link SchemaObject}.
     *
     * @param type schema object
     * @return unmodifiable set of {@link SchemaField} names in {@code type}
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public SortedSet<String> getSchemaFieldNames(SchemaObject type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final TreeMap<String, SchemaField> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return Collections.unmodifiableSortedSet(fieldMap.navigableKeySet());
    }
}

