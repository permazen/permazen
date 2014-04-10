
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Superclass for the {@link ObjType} and {@link Field} classes which make up a {@link Schema} version.
 *
 * <p>
 * Instances have a {@linkplain #getName name} (which is mostly ignored) and a unique {@linkplain #getStorageId storage ID}
 * which is used to allocate a storage area for the schema object in the key/value store's key namespace.
 * Instances are also associated with a {@linkplain #getVersion specific} {@link SchemaVersion}.
 * </p>
 */
public abstract class SchemaItem {

    final String name;
    final int storageId;
    final SchemaVersion version;

    SchemaItem(String name, int storageId, SchemaVersion version) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid storageId " + storageId);
        if (version == null)
            throw new IllegalArgumentException("null version");
        this.name = name;
        this.storageId = storageId;
        this.version = version;
    }

    /**
     * Get the associated with this instance.
     *
     * @return name of this object type or field, never null
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the storage ID associated with this instance.
     *
     * @return storage ID, always greater than zero
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the schema version with which this instance is associated.
     */
    public SchemaVersion getVersion() {
        return this.version;
    }

    @Override
    public abstract String toString();

    static Class<? extends FieldStorageInfo> infoTypeFor(Class<? extends Field<?>> type) {
        if (ReferenceField.class.isAssignableFrom(type))
            return ReferenceFieldStorageInfo.class;
        if (SimpleField.class.isAssignableFrom(type))
            return SimpleFieldStorageInfo.class;
        if (MapField.class.isAssignableFrom(type))
            return MapFieldStorageInfo.class;
        if (ListField.class.isAssignableFrom(type))
            return ListFieldStorageInfo.class;
        if (SetField.class.isAssignableFrom(type))
            return SetFieldStorageInfo.class;
        throw new IllegalArgumentException("no StorageInfo type known for " + type);
    }
}

