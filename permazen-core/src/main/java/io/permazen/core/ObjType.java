
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
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

/**
 * Represents a {@link Database} object type.
 */
public class ObjType extends SchemaItem {

    final TreeMap<String, Field<?>> fields = new TreeMap<>();                               // does not include sub-fields
    final TreeMap<Integer, Field<?>> fieldsByStorageId = new TreeMap<>();                   // does not include sub-fields
    final TreeMap<String, Field<?>> fieldsAndSubFields = new TreeMap<>();                   // includes sub-fields
    final TreeMap<String, ComplexField<?>> complexFields = new TreeMap<>();
    final TreeMap<String, SimpleField<?>> simpleFields = new TreeMap<>();                   // does not include sub-fields
    final HashSet<SimpleField<?>> indexedSimpleFields = new HashSet<>();                    // does not include sub-fields
    final TreeMap<String, CounterField> counterFields = new TreeMap<>();
    final TreeMap<String, ReferenceField> referenceFieldsAndSubFields = new TreeMap<>();    // includes sub-fields
    final TreeMap<String, CompositeIndex> compositeIndexes = new TreeMap<>();               // composite indexes

// Constructor

    /**
     * Constructor.
     */
    ObjType(Schema schema, SchemaObjectType schemaType) {
        super(schema, schemaType, schemaType.getName());
    }

// Initialization

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
            this.fieldsByStorageId.put(field.storageId, field);
            this.fieldsAndSubFields.put(field.name, field);

            // Populate maps from simple fields
            if (field instanceof SimpleField) {
                final SimpleField<?> simpleField = (SimpleField<?>)field;
                final Encoding<?> encoding = simpleField.encoding;

                // Verify simple field encodings have default values
                Object obj;
                try {
                    obj = encoding.getDefaultValue();
                } catch (UnsupportedOperationException e) {
                    obj = null;
                }
                if (obj == null && !encoding.supportsNull()) {
                    throw new InvalidSchemaException(
                      String.format("encoding \"%s\" for field \"%s\" in type \"%s\" has no default value",
                        encoding.getEncodingId(), simpleField.name, this.name));
                }

                // Add to maps
                this.simpleFields.put(simpleField.name, simpleField);
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

// Public Methods

    /**
     * Create a new database object of this type in the given transaction.
     *
     * @param tx transaction
     * @return new instance of this type
     * @throws UnknownTypeException if this object type does not exist in the given transaction
     * @throws StaleTransactionException if {@code tx} no longer usable
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public ObjId create(Transaction tx) {
        Preconditions.checkArgument(tx != null, "null tx");
        return tx.create(this.name, this.schema.getSchemaId());
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
     * @throws UnknownFieldException if no such field exists
     */
    public Field<?> getField(String name) {
        final Field<?> field = this.fieldsAndSubFields.get(name);
        if (field == null)
            throw new UnknownFieldException(this, name);
        return field;
    }

    /**
     * Get the {@link Field} in this instance with the given storage ID.
     *
     * <p>
     * This does not find sub-fields.
     *
     * @param storageId field storage ID
     * @return the corresponding {@link Field}
     * @throws UnknownFieldException if no such field exists
     */
    public Field<?> getField(int storageId) {
        final Field<?> field = this.fieldsByStorageId.get(storageId);
        if (field == null)
            throw new UnknownFieldException(this, storageId);
        return field;
    }

    /**
     * Get all fields associated with this object type keyed by name. Includes sub-fields of complex fields.
     *
     * @return unmodifiable mapping from field full name to field
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

// Object

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
