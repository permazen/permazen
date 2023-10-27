
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaModel;
import io.permazen.schema.SchemaObjectType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Contains information describing one schema version.
 */
public class Schema {

    final int versionNumber;
    final byte[] encodedXML;
    final SchemaModel schemaModel;
    final TreeMap<Integer, ObjType> objTypeMap = new TreeMap<>();
    final TreeMap<Integer, StorageInfo> storageInfoMap = new TreeMap<>();
    final ArrayList<HashMap<ReferenceField, KeyRanges>> deleteActionKeyRanges = new ArrayList<>(DeleteAction.values().length);

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if a storage ID is used twice
     */
    Schema(int versionNumber, byte[] encodedXML, SchemaModel schemaModel, EncodingRegistry encodingRegistry) {

        // Sanity check
        Preconditions.checkArgument(versionNumber > 0, "non-positive versionNumber");
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");
        this.versionNumber = versionNumber;
        this.encodedXML = encodedXML;
        final SchemaModel lockedDownSchemaModel;
        if (schemaModel.isLockedDown())
            lockedDownSchemaModel = schemaModel;
        else {
            lockedDownSchemaModel = schemaModel.clone();
            lockedDownSchemaModel.lockDown();
        }
        this.schemaModel = lockedDownSchemaModel;

        // Build object types
        for (SchemaObjectType schemaObjectType : this.schemaModel.getSchemaObjectTypes().values()) {
            final ObjType objType = new ObjType(schemaObjectType, this, encodingRegistry);
            final int storageId = objType.getStorageId();
            final ObjType otherObjType = this.objTypeMap.put(storageId, objType);
            if (otherObjType != null) {
                throw new IllegalArgumentException("incompatible use of storage ID " + storageId
                  + " by both " + otherObjType + " and " + objType + " in " + this);
            }
        }

        // Create StorageInfo objects and check for conflicts
        final HashMap<Integer, String> descriptionMap = new HashMap<>();
        for (ObjType objType : this.objTypeMap.values()) {
            this.addStorageInfo(objType, descriptionMap);
            for (Field<?> field : objType.fields.values()) {
                this.addStorageInfo(field, descriptionMap);
                if (field instanceof ComplexField) {
                    final ComplexField<?> complexField = (ComplexField<?>)field;
                    for (SimpleField<?> subField : complexField.getSubFields())
                        this.addStorageInfo(subField, descriptionMap);
                }
            }
            for (CompositeIndex index : objType.compositeIndexes.values())
                this.addStorageInfo(index, descriptionMap);
        }

        // This is an optimization for handling DeleteAction. Because the same reference field can be configured with a
        // different DeleteAction in different object types, which can come from the same or a different schema version,
        // when an object is deleted, we have to be careful to only apply DeleteAction's matching the schema version and
        // object type of the referring fields. What follows builds a data structure to optimize finding them at runtime...

        // First calculate, for each reference field, the KeyRanges covering all object types that contain the field
        final HashMap<ReferenceField, KeyRanges> allObjTypesKeyRangesMap = new HashMap<>();
        for (ObjType objType : this.objTypeMap.values()) {
            final KeyRange objTypeKeyRange = ObjId.getKeyRange(objType.storageId);
            for (ReferenceField field : objType.referenceFieldsAndSubFields.values())
                allObjTypesKeyRangesMap.computeIfAbsent(field, f -> KeyRanges.empty()).add(objTypeKeyRange);
        }

        // Now do the same thing again, but broken out by the reference fields' configured DeleteAction
        for (DeleteAction deleteAction : DeleteAction.values()) {

            // Find reference fields matching the DeleteAction
            final HashMap<ReferenceField, KeyRanges> fieldKeyRanges = new HashMap<>();
            for (ObjType objType : this.objTypeMap.values()) {
                final KeyRange objTypeKeyRange = ObjId.getKeyRange(objType.storageId);
                for (ReferenceField field : objType.referenceFieldsAndSubFields.values()) {
                    if (field.onDelete.equals(deleteAction))
                        fieldKeyRanges.computeIfAbsent(field, i -> KeyRanges.empty()).add(objTypeKeyRange);
                }
            }

            // If for any field, the field is configured the same way in every object type, then no need to restrict by object type
            for (Map.Entry<ReferenceField, KeyRanges> entry : fieldKeyRanges.entrySet()) {
                final ReferenceField field = entry.getKey();
                final KeyRanges keyRanges = entry.getValue();
                if (keyRanges.equals(allObjTypesKeyRangesMap.get(field)))
                    entry.setValue(null);
            }

            // Done
            this.deleteActionKeyRanges.add(fieldKeyRanges);
        }
    }

    /**
     * Get the version number associated with this instance.
     *
     * @return version number, always greater than zero
     */
    public int getVersionNumber() {
        return this.versionNumber;
    }

    /**
     * Get the {@link SchemaModel} on which this schema version is based.
     *
     * @return schema model
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get all of the {@link ObjType}s that constitute this schema version, indexed by storage ID.
     *
     * @return unmodifiable mapping from {@linkplain ObjType#getStorageId storage ID} to {@link ObjType}
     */
    public SortedMap<Integer, ObjType> getObjTypes() {
        return Collections.unmodifiableSortedMap(this.objTypeMap);
    }

    /**
     * Get the {@link ObjType} in this schema with the given storage ID.
     *
     * @param storageId storage ID
     * @return the {@link ObjType} with storage ID {@code storageID}
     * @throws UnknownTypeException if no {@link ObjType} with storage ID {@code storageId} exists
     */
    public ObjType getObjType(int storageId) {
        final ObjType objType = this.objTypeMap.get(storageId);
        if (objType == null)
            throw new UnknownTypeException(storageId, this.versionNumber);
        return objType;
    }

    @Override
    public String toString() {
        return "schema version " + this.versionNumber;
    }

    // Add new StorageInfo, checking for storage conflicts
    private void addStorageInfo(SchemaItem schemaItem, Map<Integer, String> descriptionMap) {
        final StorageInfo storageInfo = schemaItem.toStorageInfo();
        if (storageInfo == null)
            return;
        final StorageInfo previous = this.storageInfoMap.put(storageInfo.storageId, storageInfo);
        if (previous != null && !previous.equals(storageInfo)) {
            throw new IllegalArgumentException("incompatible use of storage ID " + storageInfo.storageId
              + " for both " + descriptionMap.get(storageInfo.storageId) + " and " + schemaItem);
        }
        descriptionMap.put(storageInfo.storageId, "" + schemaItem);
    }
}
