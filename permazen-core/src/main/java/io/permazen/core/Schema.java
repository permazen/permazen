
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.schema.SchemaObjectType;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Reflects one {@link Schema} recorded in a {@link Database} as seen by a particular {@link Transaction}.
 *
 * <p>
 * Instances are immutable and thread safe.
 */
public class Schema {

    private final int schemaIndex;
    private final SchemaModel schemaModel;
    private final SchemaBundle schemaBundle;
    private final TreeMap<String, ObjType> objTypesByName = new TreeMap<>();
    private final HashMap<Integer, ObjType> objTypesByStorageId = new HashMap<>();

    // Maps each DeleteAction to a map from reference field to key ranges of object types that have the field with that action
    private final EnumMap<DeleteAction, Map<ReferenceField, KeyRanges>> deleteActionKeyRanges = new EnumMap<>(DeleteAction.class);

// Constructor

    Schema(SchemaBundle schemaBundle, int schemaIndex, SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(schemaBundle != null, "null schemaBundle");
        Preconditions.checkArgument(schemaIndex > 0, "non-positive schemaIndex");
        Preconditions.checkArgument(schemaModel.isLockedDown(), "schemaModel not locked down");
        schemaModel.validate();                             // this should already be done so this should be quick

        // Initialize fields
        this.schemaIndex = schemaIndex;
        this.schemaModel = schemaModel;
        this.schemaBundle = schemaBundle;

        // Create object types but don't initialize them yet
        for (SchemaObjectType schemaType : this.schemaModel.getSchemaObjectTypes().values()) {
            final ObjType objType = new ObjType(this, schemaType);
            assert !this.objTypesByName.containsKey(objType.getName());
            assert !this.objTypesByStorageId.containsKey(objType.getStorageId());
            this.objTypesByName.put(objType.getName(), objType);
            this.objTypesByStorageId.put(objType.getStorageId(), objType);
        }
    }

// Initialization

    void initialize(EncodingRegistry encodingRegistry) {

        // Sanity check
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");

        // Initialize object types
        for (SchemaObjectType schemaType : this.schemaModel.getSchemaObjectTypes().values()) {
            final ObjType objType = this.objTypesByName.get(schemaType.getName());
            objType.initialize(schemaType, encodingRegistry);
        }

        // This is an optimization for handling DeleteAction. Because the same reference field can be configured with
        // a different DeleteAction in different object types, which can come from the same or different schemas,
        // when an object is deleted, we have to be careful to only apply DeleteAction's matching the schema and object
        // type of the referring fields. What follows builds a data structure to optimize finding them at runtime.

        // First calculate, for each reference field, KeyRanges covering all object types that contain the field
        final HashMap<ReferenceField, KeyRanges> allObjTypesKeyRangesMap = new HashMap<>();
        this.objTypesByStorageId.forEach((storageId, objType) -> {
            final KeyRange objTypeKeyRange = ObjId.getKeyRange((int)storageId);
            for (ReferenceField field : objType.referenceFieldsAndSubFields.values())
                allObjTypesKeyRangesMap.computeIfAbsent(field, f -> KeyRanges.empty()).add(objTypeKeyRange);
        });

        // Now do the same thing again, but broken out by the reference fields' configured DeleteAction
        for (DeleteAction deleteAction : DeleteAction.values()) {

            // Find reference fields matching the DeleteAction
            final HashMap<ReferenceField, KeyRanges> fieldKeyRanges = new HashMap<>();
            this.objTypesByStorageId.forEach((storageId, objType) -> {
                final KeyRange objTypeKeyRange = ObjId.getKeyRange((int)storageId);
                for (ReferenceField field : objType.referenceFieldsAndSubFields.values()) {
                    if (field.inverseDelete.equals(deleteAction))
                        fieldKeyRanges.computeIfAbsent(field, i -> KeyRanges.empty()).add(objTypeKeyRange);
                }
            });

            // If for any field, the field is configured the same way in every object type, then no need to restrict by object type
            fieldKeyRanges.entrySet().forEach(entry -> {
                if (entry.getValue().equals(allObjTypesKeyRangesMap.get(entry.getKey())))
                    entry.setValue(null);
            });

            // Done
            this.deleteActionKeyRanges.put(deleteAction, fieldKeyRanges);
        }
    }

// Public Methods

    /**
     * Get the schema bundle that this instance is a member of.
     *
     * @return the containing schema bundle
     */
    public SchemaBundle getSchemaBundle() {
        return this.schemaBundle;
    }

    /**
     * Get the schema index associated with this instance.
     *
     * @return schema index
     */
    public int getSchemaIndex() {
        return this.schemaIndex;
    }

    /**
     * Get the schema ID associated with this instance.
     *
     * @return schema ID
     */
    public SchemaId getSchemaId() {
        return this.schemaModel.getSchemaId();
    }

    /**
     * Get the original {@link SchemaModel} on which this instance is based as it is recorded in the database.
     *
     * <p>
     * Equivalent to: {@link getSchemaModel(boolean)}{@code (false)}.
     *
     * @return schema model
     */
    public SchemaModel getSchemaModel() {
        return this.getSchemaModel(false);
    }

    /**
     * Get the {@link SchemaModel} on which this instance is based, optionally including explicit storage ID assignments.
     *
     * <p>
     * If {@code withStorageIds} is true, then all of the schema items in the returned model will
     * have non-zero storage ID's reflecting their storage ID assignments in the database.
     *
     * @param withStorageIds true to include all storage ID assignments, false for the original model
     * @return schema model with storage ID assignments
     */
    public SchemaModel getSchemaModel(boolean withStorageIds) {
        if (!withStorageIds)
            return this.schemaModel;
        final SchemaModel model = this.schemaModel.clone();
        model.visitSchemaItems(item -> item.setStorageId(this.schemaBundle.getStorageId(item.getSchemaId())));
        return model;
    }

    /**
     * Get all of the {@link ObjType}s that constitute this schema, indexed by type name.
     *
     * @return unmodifiable mapping from {@linkplain ObjType#getName type name} to {@link ObjType}
     */
    public NavigableMap<String, ObjType> getObjTypes() {
        return Collections.unmodifiableNavigableMap(this.objTypesByName);
    }

    /**
     * Get the {@link ObjType} in this schema with the given name.
     *
     * @param typeName object type name
     * @return the corresponding {@link ObjType}
     * @throws UnknownTypeException if no such {@link ObjType} exists
     * @throws IllegalArgumentException if {@code typeName} is null
     */
    public ObjType getObjType(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        final ObjType objType = this.objTypesByName.get(typeName);
        if (objType == null)
            throw new UnknownTypeException(typeName, this);
        return objType;
    }

    /**
     * Get the {@link ObjType} in this schema with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return the corresponding {@link ObjType}
     * @throws UnknownTypeException if no such {@link ObjType} exists
     * @throws IllegalArgumentException if {@code storageId} is invalid
     */
    public ObjType getObjType(int storageId) {
        Preconditions.checkArgument(storageId > 0, "invalid storageId");
        final ObjType objType = this.objTypesByStorageId.get(storageId);
        if (objType == null)
            throw new UnknownTypeException("#" + storageId, this);
        return objType;
    }

// Object

    @Override
    public String toString() {
        return this.schemaModel.getSchemaId() + "@" + this.schemaIndex;
    }

// Package Methods

    EnumMap<DeleteAction, Map<ReferenceField, KeyRanges>> getDeleteActionKeyRanges() {
        return this.deleteActionKeyRanges;
    }
}
