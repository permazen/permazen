
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObjectType;

/**
 * Contains information describing one {@link Schema} version.
 */
public class SchemaVersion {

    final int versionNumber;
    final byte[] encodedXML;
    final SchemaModel schemaModel;
    final TreeMap<Integer, ObjType> objTypeMap = new TreeMap<>();
    final TreeMap<Integer, StorageInfo> storageInfoMap = new TreeMap<>();

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if a storage ID is used twice
     */
    SchemaVersion(int versionNumber, byte[] encodedXML, SchemaModel schemaModel, FieldTypeRegistry fieldTypeRegistry) {

        // Sanity check
        if (versionNumber <= 0)
            throw new IllegalArgumentException("versionNumber <= 0");
        if (schemaModel == null)
            throw new IllegalArgumentException("null schemaModel");
        this.versionNumber = versionNumber;
        this.encodedXML = encodedXML;
        this.schemaModel = schemaModel.clone();

        // Build object types
        for (SchemaObjectType schemaObjectType : this.schemaModel.getSchemaObjectTypes().values()) {
            final ObjType objType = new ObjType(schemaObjectType, this, fieldTypeRegistry);
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

    // Add new StorageInfo, checking for conflicts
    private void addStorageInfo(SchemaItem schemaItem, Map<Integer, String> descriptionMap) {
        final StorageInfo storageInfo = schemaItem.toStorageInfo();
        final StorageInfo previous = this.storageInfoMap.put(storageInfo.storageId, storageInfo);
        if (previous != null && !previous.equals(storageInfo)) {
            throw new IllegalArgumentException("invalid duplicate use of storage ID " + storageInfo.storageId
              + " for both " + descriptionMap.get(storageInfo.storageId) + " and " + schemaItem);
        }
        descriptionMap.put(storageInfo.storageId, "" + schemaItem);
    }
}

