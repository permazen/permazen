
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.EncodingRegistry;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.schema.SchemaField;
import io.permazen.schema.SchemaObjectType;
import io.permazen.schema.SimpleSchemaField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final TreeMap<String, Field<?>> fields = new TreeMap<>();                               // does not include sub-fields
    final TreeMap<String, Field<?>> fieldsAndSubFields = new TreeMap<>();                   // includes sub-fields
    final TreeMap<String, ComplexField<?>> complexFields = new TreeMap<>();
    final TreeMap<String, SimpleField<?>> simpleFields = new TreeMap<>();                   // does not include sub-fields
    final HashSet<SimpleField<?>> indexedSimpleFields = new HashSet<>();                    // does not include sub-fields
    final TreeMap<String, CounterField> counterFields = new TreeMap<>();
    final TreeMap<String, ReferenceField> referenceFieldsAndSubFields = new TreeMap<>();    // includes sub-fields
    final TreeSet<Integer> fieldStorageIds = new TreeSet<>();                               // does not include sub-fields
    final TreeMap<String, CompositeIndex> compositeIndexes = new TreeMap<>();               // composite indexes

    /**
     * Constructor.
     */
    ObjType(Schema schema, SchemaObjectType schemaType) {
        super(schema, schemaType, schemaType.getName());
    }

    /**
     * Initialization.
     */
    void initialize(SchemaObjectType schemaType, EncodingRegistry encodingRegistry) {

        // Sanity check
        Preconditions.checkArgument(schemaType != null, "null schemaType");
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");

        // Build fields and populate various maps
        final FieldBuilder fieldBuilder = new FieldBuilder(this, encodingRegistry);
        for (SchemaField schemaField : schemaType.getSchemaFields().values()) {

            // Build field
            final Field<?> field = schemaField.visit(fieldBuilder);
            this.fields.put(field.name, field);
            this.fieldStorageIds.add(field.storageId);
            this.fieldsAndSubFields.put(field.name, field);

            // Populate maps from simple fields
            if (field instanceof SimpleField) {
                final SimpleField<?> simpleField = (SimpleField<?>)field;
                this.simpleFields.put(field.name, simpleField);
                if (simpleField.indexed)
                    this.indexedSimpleFields.add(simpleField);
            }

            // Populate maps from complex fields
            if (field instanceof ComplexField) {
                final ComplexField<?> complexField = (ComplexField<?>)field;
                this.complexFields.put(field.name, complexField);
                for (SimpleField<?> subField : complexField.getSubFields()) {
                    final String subFieldName = complexField.name + "." + subField.name;
                    this.fieldsAndSubFields.put(subFieldName, subField);
                    if (subField instanceof ReferenceField)
                        this.referenceFieldsAndSubFields.put(subFieldName, (ReferenceField)subField);
                }
            }

            // Populate maps from counter fields
            if (field instanceof CounterField)
                this.counterFields.put(field.name, (CounterField)field);

            // Populate maps from reference fields
            if (field instanceof ReferenceField)
                this.referenceFieldsAndSubFields.put(field.name, (ReferenceField)field);
        }

        // Build simple indexes and link simple fields to them
        for (Field<?> field : this.fieldsAndSubFields.values()) {
            if (!(field instanceof SimpleField))
                continue;
            final SimpleField<?> simpleField = (SimpleField)field;
            if (!simpleField.indexed)
                continue;
            this.createAndLinkSimpleIndex(schema, fieldBuilder.simpleModels.get(simpleField), simpleField);
        }

        // Build composite indexes
        for (SchemaCompositeIndex schemaIndex : schemaType.getSchemaCompositeIndexes().values()) {
            final List<SimpleField<?>> fieldList = this.getCompositeIndexFields(schemaIndex);
            final CompositeIndex index = new CompositeIndex(schema, schemaIndex, this, fieldList);
            this.compositeIndexes.put(index.name, index);
        }

        // Link simple fields to all composite indexes that include them
        for (Field<?> field : this.fieldsAndSubFields.values()) {
            if (!(field instanceof SimpleField))
                continue;
            final SimpleField<?> simpleField = (SimpleField)field;
            for (CompositeIndex index : this.compositeIndexes.values()) {
                final int posn = index.fields.indexOf(simpleField);
                if (posn != -1) {
                    if (simpleField.compositeIndexMap == null)
                        simpleField.compositeIndexMap = new HashMap<>(2);
                    simpleField.compositeIndexMap.put(index, posn);
                }
            }
        }
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private <T> SimpleIndex<T> createAndLinkSimpleIndex(Schema schema, SimpleSchemaField schemaField, SimpleField<T> field) {
        Preconditions.checkArgument(schemaField != null, "null schemaField");

        // Create index
        final SimpleIndex<T> index = field.parent != null ?
          (SimpleIndex<T>)field.parent.createSubFieldIndex(schema, schemaField, this, field) :
          new SimpleFieldIndex<T>(schema, schemaField, this, field);

        // Link field to it
        assert field.index == null;
        field.index = index;

        // Done
        return index;
    }

    /**
     * Get all fields associated with this object type keyed by name. Does not include sub-fields of complex fields.
     *
     * @return unmodifiable mapping from field name to field
     */
    public NavigableMap<String, Field<?>> getFields() {
        return Collections.unmodifiableNavigableMap(this.fields);
    }

    /**
     * Get the {@link Field} in this instance with the given name.
     *
     * <p>
     * For complex sub-fields, specify the name like {@code mymap.key}.
     *
     * @param name field name
     * @return the named {@link Field}
     * @throws UnknownFieldException if such field exists
     */
    public Field<?> getField(String name) {
        final Field<?> field = this.fieldsAndSubFields.get(name);
        if (field == null)
            throw new UnknownFieldException(this, name, "field");
        return field;
    }

    /**
     * Get all fields associated with this object type keyed by name. Includes sub-fields of complex fields.
     *
     * @return unmodifiable mapping from field name to field
     */
    public NavigableMap<String, Field<?>> getFieldsAndSubFields() {
        return Collections.unmodifiableNavigableMap(this.fieldsAndSubFields);
    }

    /**
     * Get all composite indexes associated with this object type keyed by name.
     *
     * @return unmodifiable mapping from name to composite index
     */
    public NavigableMap<String, CompositeIndex> getCompositeIndexes() {
        return Collections.unmodifiableNavigableMap(this.compositeIndexes);
    }

    /**
     * Get the {@link CompositeIndex} associated with this instance with the given name.
     *
     * @param name index name
     * @return the named {@link CompositeIndex}
     * @throws UnknownIndexException if no such index exists
     */
    public CompositeIndex getCompositeIndex(String name) {
        final CompositeIndex index = this.compositeIndexes.get(name);
        if (index == null)
            throw new UnknownIndexException(name, String.format("no composite index named \"%s\" exists", name));
        return index;
    }

    @Override
    public String toString() {
        return String.format("object type \"%s\" in %s", this.name, this.schema);
    }

// Internal methods

    private List<SimpleField<?>> getCompositeIndexFields(SchemaCompositeIndex index) {
        final List<SimpleField<?>> fieldList = new ArrayList<>(index.getIndexedFields().size());
        for (String fieldName : index.getIndexedFields()) {
            final SimpleField<?> field = (SimpleField<?>)this.fields.get(fieldName);
            if (field == null) {
                throw new IllegalArgumentException(String.format(
                  "index \"%s\" contains invalid field \"%s\"", index.getName(), fieldName));
            }
            fieldList.add(field);
        }
        assert fieldList.size() > 1;
        return fieldList;
    }
}
