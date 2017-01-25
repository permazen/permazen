
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import org.jsimpledb.schema.AbstractSchemaItem;

/**
 * Superclass for the {@link JClass} and {@link JField} classes which define the schema
 * associated with a {@link JSimpleDB}.
 */
public abstract class JSchemaObject {

    final JSimpleDB jdb;
    final String name;
    final int storageId;
    final String description;

    JSchemaObject(JSimpleDB jdb, String name, int storageId, String description) {
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
     * Get the {@link JSimpleDB} with which this instance is associated.
     *
     * @return the associated database
     */
    public JSimpleDB getJSimpleDB() {
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

    abstract AbstractSchemaItem toSchemaItem(JSimpleDB jdb);

    void initialize(JSimpleDB jdb, AbstractSchemaItem schemaItem) {
        schemaItem.setName(this.name);
        schemaItem.setStorageId(this.storageId);
    }

    @Override
    public String toString() {
        return this.description;
    }
}

