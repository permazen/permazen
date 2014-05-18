
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
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
    private final TreeMap<String, LinkedHashSet<SchemaObject>> typeMap = new TreeMap<>();
    private final TreeMap<Integer, TreeMap<String, LinkedHashSet<SchemaField>>> typeFieldMap = new TreeMap<>();

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
            LinkedHashSet<SchemaObject> typeSet = this.typeMap.get(type.getName());
            if (typeSet == null) {
                typeSet = new LinkedHashSet<SchemaObject>();
                this.typeMap.put(type.getName(), typeSet);
            }
            typeSet.add(type);

            // Index field names
            final TreeMap<String, LinkedHashSet<SchemaField>> fieldMap = new TreeMap<>();
            this.typeFieldMap.put(type.getStorageId(), fieldMap);
            for (SchemaField field : type.getSchemaFields().values()) {

                // Index field name
                LinkedHashSet<SchemaField> fieldSet = fieldMap.get(field.getName());
                if (fieldSet == null) {
                    fieldSet = new LinkedHashSet<SchemaField>();
                    fieldMap.put(field.getName(), fieldSet);
                }
                fieldSet.add(field);
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
     * Get all {@link SchemaObject}(s) with the given name.
     *
     * @param name type name
     * @throws IllegalArgumentException if {@code name} is null
     * @return unmodifiable set containing zero or more {@link SchemaObject}s with name {@code name}
     */
    public Set<SchemaObject> getSchemaObjects(String name) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        final LinkedHashSet<SchemaObject> typeSet = this.typeMap.get(name);
        return typeSet != null ? Collections.unmodifiableSet(typeSet) : Collections.<SchemaObject>emptySet();
    }

    /**
     * Get the unique {@link SchemaObject} with the given name.
     *
     * @param name type name
     * @return the unique {@link SchemaObject} with name {@code name}
     * @throws IllegalArgumentException if {@code name} is null
     * @throws IllegalArgumentException if zero, or more than one, {@link SchemaObject}s exist with name {@code name}
     */
    public SchemaObject getSchemaObject(String name) {
        final Set<SchemaObject> typeSet = this.getSchemaObjects(name);
        switch (typeSet.size()) {
        case 0:
            throw new IllegalArgumentException("no object type exists with name `" + name + "'");
        case 1:
            return typeSet.iterator().next();
        default:
            throw new IllegalArgumentException("mulitple object types exist with name `" + name + "'");
        }
    }

    /**
     * Get all of the names of {@link SchemaObject}s.
     *
     * @return unmodifiable set of {@link SchemaObject} names
     */
    public SortedSet<String> getSchemaObjectNames() {
        return Collections.unmodifiableSortedSet(this.typeMap.navigableKeySet());
    }

    /**
     * Get all {@link SchemaField}(s) with the given name in the given {@link SchemaObject}.
     *
     * @param type object type
     * @param name field name
     * @throws IllegalArgumentException if either paramter is null
     * @return unmodifiable set containing zero or more {@link SchemaField}s with name {@code name} in {@code type}
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     */
    public Set<SchemaField> getSchemaFields(SchemaObject type, String name) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (name == null)
            throw new IllegalArgumentException("null name");
        final TreeMap<String, LinkedHashSet<SchemaField>> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        final LinkedHashSet<SchemaField> fieldSet = fieldMap.get(name);
        return fieldSet != null ? Collections.unmodifiableSet(fieldSet) : Collections.<SchemaField>emptySet();
    }

    /**
     * Get the unique {@link SchemaField} with the given name.
     *
     * @param type object type
     * @param name field name
     * @throws IllegalArgumentException if either paramter is null
     * @return the unique {@link SchemaField} with name {@code name} in {@code type}
     * @throws IllegalArgumentException if {@code type} is not indexed by this instance
     * @throws IllegalArgumentException if zero, or more than one, {@link SchemaField}s exist with name {@code name} in {@code type}
     */
    public SchemaField getSchemaField(SchemaObject type, String name) {
        final Set<SchemaField> fieldSet = this.getSchemaFields(type, name);
        switch (fieldSet.size()) {
        case 0:
            throw new IllegalArgumentException("no field exists with name `" + name + "' in type `" + type.getName() + "'");
        case 1:
            return fieldSet.iterator().next();
        default:
            throw new IllegalArgumentException("mulitple fields exist with name `" + name + "' in type `" + type.getName() + "'");
        }
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
        final TreeMap<String, LinkedHashSet<SchemaField>> fieldMap = this.typeFieldMap.get(type.getStorageId());
        if (fieldMap == null)
            throw new IllegalArgumentException("unknown type `" + type.getName() + "' with storage ID " + type.getStorageId());
        return Collections.unmodifiableSortedSet(fieldMap.navigableKeySet());
    }
}

