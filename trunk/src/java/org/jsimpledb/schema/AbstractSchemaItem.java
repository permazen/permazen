
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

/**
 * Common superclass for {@link SchemaObject} and {@link SchemaField}.
 */
public class AbstractSchemaItem implements Cloneable {

    private String name;
    private int storageId;

    /**
     * Get the name associated with this instance.
     */
    @Size(min = 1, message = "names cannot be empty")
    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the storage ID associated with this instance.
     * Storage IDs must be positive values.
     */
    @Min(value = 1, message = "a positive storage ID is required")
    public int getStorageId() {
        return this.storageId;
    }
    public void setStorageId(int storageId) {
        this.storageId = storageId;
    }

// Object

    @Override
    public String toString() {
        return "#" + this.storageId + (this.name != null ? " `" + this.name + "'" : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractSchemaItem that = (AbstractSchemaItem)obj;
        return (this.name != null ? this.name.equals(that.name) : that.name == null) && this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return (this.name != null ? this.name.hashCode() : 0) ^ this.storageId;
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    public AbstractSchemaItem clone() {
        try {
            return (AbstractSchemaItem)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}

