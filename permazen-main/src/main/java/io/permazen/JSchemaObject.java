
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.schema.AbstractSchemaItem;

/**
 * Superclass for the {@link JClass} and {@link JField} classes which define the schema
 * associated with a {@link Permazen}.
 */
public abstract class JSchemaObject {

    final Permazen jdb;
    final String name;
    final int storageId;
    final String description;

    JSchemaObject(Permazen jdb, String name, int storageId, String description) {
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(storageId > 0, "invalid non-positive storageId");
        Preconditions.checkArgument(description != null, "null description");
        this.jdb = jdb;
        this.name = name;
        this.storageId = storageId;
        this.description = description;
    }

// Public API

    /**
     * Get the {@link Permazen} with which this instance is associated.
     *
     * @return the associated database
     */
    public Permazen getPermazen() {
        return this.jdb;
    }

    /**
     * Get the name of this instance.
     *
     * @return this instance's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the storage ID of this instance.
     *
     * @return this instance's storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }

// Internal methods

    IndexInfo toIndexInfo() {
        throw new UnsupportedOperationException();
    }

    abstract AbstractSchemaItem toSchemaItem(Permazen jdb);

    void initialize(Permazen jdb, AbstractSchemaItem schemaItem) {
        schemaItem.setName(this.name);
        schemaItem.setStorageId(this.storageId);
    }

    @Override
    public String toString() {
        return this.description;
    }
}
