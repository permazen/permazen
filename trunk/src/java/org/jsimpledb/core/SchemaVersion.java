
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObject;

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

        // Build object types and fields, and associated StorageInfo objects
        final ArrayList<ObjType> objTypes = new ArrayList<>();
        final TreeMap<Integer, Field<?>> fieldMap = new TreeMap<>();
        for (SchemaObject schemaObject : this.schemaModel.getSchemaObjects().values()) {
            final ObjType objType = new ObjType(schemaObject, this, fieldTypeRegistry);
            this.addObjType(objType, fieldMap);
            this.addStorageInfo(objType);
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

    private void addObjType(ObjType objType, TreeMap<Integer, Field<?>> fieldMap) {
        final int storageId = objType.getStorageId();
        final ObjType otherObjType = this.objTypeMap.put(storageId, objType);
        if (otherObjType != null) {
            throw new IllegalArgumentException("incompatible use of storage ID " + storageId
              + " by both " + otherObjType + " and " + objType + " in " + this);
        }
        for (Field<?> field : objType.getFieldsAndSubFields()) {
            final Field<?> otherField = fieldMap.put(field.storageId, field);
            if (otherField != null && !field.isEquivalent(otherField)) {
                throw new IllegalArgumentException("incompatible use of storage ID " + storageId
                  + " by both " + otherField + " and " + field + " in " + this);
            }
        }
    }

    private void addStorageInfo(ObjType objType) {
        final ObjTypeStorageInfo objInfo = objType.toStorageInfo();
        for (Field<?> field : objType.fields.values()) {
            final FieldStorageInfo fieldInfo = field.toStorageInfo();
            if (field instanceof ComplexField) {
                for (SimpleFieldStorageInfo subFieldInfo : ((ComplexFieldStorageInfo)fieldInfo).getSubFields())
                    this.addStorageInfo(subFieldInfo);
            }
            this.addStorageInfo(fieldInfo);
            objInfo.getFields().put(field.storageId, fieldInfo);
        }
        this.addStorageInfo(objInfo);
    }

    private void addStorageInfo(StorageInfo storageInfo) {
        final int storageId = storageInfo.getStorageId();
        final StorageInfo previous = this.storageInfoMap.put(storageId, storageInfo);
        if (previous != null) {
            try {
                previous.verifySharedStorageId(storageInfo);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("duplicate use of storage ID " + storageId, e);     // should never happen
            }
        }
    }
}

