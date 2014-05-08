
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Contains the set of all {@link SchemaVersion}s of objects visible in a {@link Transaction}.
 * All objects have an implicit schema version number, which corresponds to one of the {@link SchemaVersion}s
 * contained here and describes the object's fields.
 *
 * <p>
 * Associated with a {@link Transaction} is a {@linkplain Transaction#getSchemaVersion specific schema version}
 * that the transaction uses to access objects.
 * </p>
 */
public class Schema {

    final TreeMap<Integer, SchemaVersion> versions = new TreeMap<>();
    final TreeMap<Integer, StorageInfo> storageInfos = new TreeMap<>();
    final TreeSet<ReferenceFieldStorageInfo> referenceFieldStorageInfos = new TreeSet<>(StorageInfo.SORT_BY_STORAGE_ID);
    final TreeSet<ReferenceFieldStorageInfo> exceptionReferenceFieldStorageInfos = new TreeSet<>(StorageInfo.SORT_BY_STORAGE_ID);

    Schema(SortedMap<Integer, SchemaVersion> versions) {

        // Copy versions
        if (versions == null)
            throw new IllegalArgumentException("null versions");
        this.versions.putAll(versions);

        // Verify Versions have the right version numbers
        for (Map.Entry<Integer, SchemaVersion> entry : this.versions.entrySet()) {
            final int versionNumber = entry.getKey();
            final SchemaVersion version = entry.getValue();
            if (version == null)
                throw new IllegalArgumentException("null version");
            if (version.versionNumber != versionNumber) {
                throw new InvalidSchemaException("schema version at index "
                  + versionNumber + " has version " + version.versionNumber);
            }
        }

        // Verify all schema versions use storage IDs in a compatible way
        final HashMap<StorageInfo, Integer> versionMap = new HashMap<>();
        for (SchemaVersion version : this.versions.values()) {
            for (Map.Entry<Integer, StorageInfo> entry : version.storageInfoMap.entrySet()) {
                final int storageId = entry.getKey();
                final StorageInfo current = entry.getValue();
                final StorageInfo previous = this.storageInfos.put(storageId, current);
                if (previous != null && !previous.canShareStorageId(current)) {
                    throw new InvalidSchemaException("incompatible use of storage ID " + storageId + " for both "
                      + previous + " in schema version " + versionMap.get(previous) + " and "
                      + current + " in schema version " + version.versionNumber);
                }
                versionMap.put(current, version.versionNumber);
            }
        }

        // Derived info
        for (ReferenceFieldStorageInfo storageInfo :
          Iterables.filter(this.storageInfos.values(), ReferenceFieldStorageInfo.class)) {
            this.referenceFieldStorageInfos.add(storageInfo);
            if (storageInfo.getOnDelete() == DeleteAction.EXCEPTION)
                this.exceptionReferenceFieldStorageInfos.add(storageInfo);
        }
    }

    /**
     * Verify the given storage ID has the specified type of {@link StorageInfo} and find it.
     *
     * @param storageId schema object storage ID
     * @param expectedType expected {@link StorageInfo} type
     * @return the actual {@link StorageInfo} instance found
     * @throws UnknownFieldException if type doesn't match and {@code expectedType} is a {@link FieldStorageInfo} sub-type
     * @throws IllegalArgumentException if type doesn't match and {@code expectedType} is a not a {@link FieldStorageInfo} sub-type
     */
    <T extends StorageInfo> T verifyStorageInfo(int storageId, Class<T> expectedType) {
        final StorageInfo storageInfo = this.storageInfos.get(storageId);
        if (storageInfo != null && expectedType.isInstance(storageInfo))
            return expectedType.cast(storageInfo);
        String message = "no " + this.getDescription(expectedType) + " with storage ID " + storageId + " exists";
        if (storageInfo != null)
            message += " (found " + storageInfo + " instead)";
        if (FieldStorageInfo.class.isAssignableFrom(expectedType))
            throw new UnknownFieldException(storageId, message);
        throw new IllegalArgumentException(message);
    }

    private String getDescription(Class<? extends StorageInfo> type) {
        if (FieldStorageInfo.class.isAssignableFrom(type))
            return type.getSimpleName().replaceAll("^(.*)Field.*$", "$1").toLowerCase() + " field";
        if (ObjTypeStorageInfo.class.isAssignableFrom(type))
            return "object type";
        return type.getSimpleName();    // ???
    }

// Accessors

    /**
     * Get all schema versions.
     *
     * @return unmodifiable list of schema versions
     */
    public SortedMap<Integer, SchemaVersion> getSchemaVersions() {
        return Collections.unmodifiableSortedMap(this.versions);
    }

    /**
     * Get the {@link Version} corresponding to the given version number.
     *
     * @param versionNumber schema version number
     * @throws IllegalArgumentException if {@code versionNumber} is not known
     */
    public SchemaVersion getVersion(int versionNumber) {
        final SchemaVersion version = this.versions.get(versionNumber);
        if (version == null)
            throw new IllegalArgumentException("unknown version " + versionNumber);
        return version;
    }

    // See if we match encoded schemas read from KV transaction
    boolean isSameVersions(SortedMap<Integer, byte[]> bytesList) {
        if (bytesList.size() != this.versions.size())
            return false;
        final Iterator<Map.Entry<Integer, byte[]>> i1 = bytesList.entrySet().iterator();
        final Iterator<Map.Entry<Integer, SchemaVersion>> i2 = this.versions.entrySet().iterator();
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext() || !i2.hasNext())
                return false;
            final Map.Entry<Integer, byte[]> entry1 = i1.next();
            final Map.Entry<Integer, SchemaVersion> entry2 = i2.next();
            if (entry1.getKey() != entry2.getKey() || !Arrays.equals(entry1.getValue(), entry2.getValue().encodedXML))
                return false;
        }
        return true;
    }
}

