
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

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
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid storageId " + storageId);
        if (description == null)
            throw new IllegalArgumentException("null description");
        this.jdb = jdb;
        this.name = name;
        this.storageId = storageId;
        this.description = description;
    }

// Public API

    /**
     * Get the {@link JSimpleDB} with which this instance is associated.
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Get the name of this instance.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the storage ID of this instance.
     */
    public int getStorageId() {
        return this.storageId;
    }

// Internal methods

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

