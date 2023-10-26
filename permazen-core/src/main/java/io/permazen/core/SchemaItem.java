
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

/**
 * Superclass for the {@link ObjType}, {@link CompositeIndex}, and {@link Field} classes which make up a {@link Schema} version.
 *
 * <p>
 * Instances have a {@linkplain #getStorageId storage ID} which must be unique across the {@link Schema} version.
 * Instances also have a {@linkplain #getName name} which must be a {@linkplain #NAME_PATTERN valid Java identifier}.
 * Instances are also associated with a {@linkplain #getSchema specific} {@link Schema}.
 */
public abstract class SchemaItem {

    /**
     * The regular expression that all {@link SchemaItem} names must match. This pattern is the same as is required
     * for Java identifiers.
     */
    public static final String NAME_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";

    final String name;
    final int storageId;
    final Schema schema;

    SchemaItem(String name, int storageId, Schema schema) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(name.matches(NAME_PATTERN), "invalid name \"" + name + "\"");
        Preconditions.checkArgument(storageId > 0, "invalid non-positive storageId");
        Preconditions.checkArgument(schema != null, "null schema");
        this.name = name;
        this.storageId = storageId;
        this.schema = schema;
    }

    /**
     * Get the name associated with this instance.
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
     *
     * @return associated schema version
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Create the corresponding {@link StorageInfo} object, if any.
     *
     * <p>
     * This should only return a non-null value if this instance's storage ID is used as the prefix
     * of some key in the database. In the case of simple fields, this implies the field must be indexed.
     */
    abstract StorageInfo toStorageInfo();

    @Override
    public abstract String toString();
}

