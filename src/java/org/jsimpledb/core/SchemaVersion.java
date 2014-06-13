
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
    final TreeMap<Integer, SchemaItem> schemaItemMap = new TreeMap<>();
    final TreeMap<Integer, StorageInfo> storageInfoMap = new TreeMap<>();
    final TreeMap<Integer, ReferenceField> referenceFields = new TreeMap<>();

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
        for (SchemaObject schemaObject : this.schemaModel.getSchemaObjects().values()) {
            final ObjType objType = new ObjType(schemaObject, this, fieldTypeRegistry);
            this.addSchemaItem(objType);
            for (Field<?> field : objType.getFieldsAndSubFields()) {
                this.addSchemaItem(field);
                if (field instanceof ReferenceField)
                    this.referenceFields.put(field.storageId, (ReferenceField)field);
            }
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
     * Get all of the {@link SchemaItem}s that constitute this schema version, indexed by storage ID.
     *
     * @return unmodifiable mapping from {@linkplain ObjType#getStorageId storage ID} to schema object
     */
    public SortedMap<Integer, SchemaItem> getSchemaItemMap() {
        return Collections.unmodifiableSortedMap(this.schemaItemMap);
    }

    /**
     * Get the {@link SchemaItem} with the given storage ID and having the specified type.
     *
     * @param storageId storage ID
     * @param type expected type
     * @return the {@link SchemaItem} with storage ID {@code storageID} and having type {@code type}
     * @throws IllegalArgumentException if none such exists or it is not of type {@code type}
     */
    public <T extends SchemaItem> T getSchemaItem(int storageId, Class<T> type) {
        final SchemaItem schemaItem = this.schemaItemMap.get(storageId);
        if (schemaItem == null)
            throw new IllegalArgumentException("storage ID " + storageId + " does not exist in " + this);
        try {
            return type.cast(schemaItem);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("storage ID " + storageId + " does not correspond to any "
              + this.getDescription(type) + " in " + this + " (found " + this.getDescription(schemaItem.getClass())
              + " instead)");
        }
    }

    @Override
    public String toString() {
        return "schema version " + this.versionNumber;
    }

    private void addSchemaItem(SchemaItem schemaItem) {
        final int storageId = schemaItem.getStorageId();
        final SchemaItem previous = this.schemaItemMap.put(storageId, schemaItem);
        if (previous != null) {

            // Allow the field with the same storage ID to appear in mulitple object types if equivalent
            if (schemaItem instanceof Field && previous instanceof Field && ((Field<?>)schemaItem).isEquivalent((Field<?>)previous))
                return;

            // Not the same
            throw new IllegalArgumentException("incompatible use of storage ID " + storageId
              + " by both " + previous + " and " + schemaItem + " in " + this);
        }
    }

    private void addStorageInfo(ObjType objType) {
        final ObjTypeStorageInfo objInfo = objType.toStorageInfo();
        this.addStorageInfo(objInfo);
        for (SimpleField<?> field : objType.simpleFields.values()) {
            final SimpleFieldStorageInfo fieldInfo = field.toStorageInfo();
            this.addStorageInfo(fieldInfo);
            objInfo.getFields().put(field.storageId, fieldInfo);
        }
        for (ComplexField<?> field : objType.complexFields.values()) {
            final ComplexFieldStorageInfo fieldInfo = field.toStorageInfo();
            objInfo.getFields().put(field.storageId, fieldInfo);
            final ArrayList<SimpleFieldStorageInfo> subFieldInfos = new ArrayList<>();
            for (SimpleField<?> subField : field.getSubFields()) {
                final SimpleFieldStorageInfo subFieldInfo = subField.toStorageInfo();
                this.addStorageInfo(subFieldInfo);
                subFieldInfos.add(subFieldInfo);
            }
            fieldInfo.setSubFields(subFieldInfos);
            this.addStorageInfo(fieldInfo);
        }
        for (CounterField field : objType.counterFields.values()) {
            final CounterFieldStorageInfo fieldInfo = field.toStorageInfo();
            this.addStorageInfo(fieldInfo);
            objInfo.getFields().put(field.storageId, fieldInfo);
        }
    }

    private void addStorageInfo(StorageInfo storageInfo) {
        final int storageId = storageInfo.getStorageId();
        final StorageInfo previous = this.storageInfoMap.put(storageId, storageInfo);
        if (previous != null && !previous.canShareStorageId(storageInfo))
            throw new IllegalArgumentException("duplicate use of storage ID " + storageId);     // should never happen
    }

    private String getDescription(Class<? extends SchemaItem> type) {
        if (type == ObjType.class)
            return "object";
        if (type == CounterField.class)
            return "counter field";
        if (type == SimpleField.class)
            return "simple field";
        if (type == ReferenceField.class)
            return "reference field";
        if (type == SetField.class)
            return "set field";
        if (type == ListField.class)
            return "list field";
        if (type == MapField.class)
            return "map field";
        return type.getSimpleName();
    }
}

