
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.validation.constraints.NotNull;

/**
 * A simple field in a {@link SchemaObject}.
 */
public class SimpleSchemaField extends SchemaField {

    private String type;
    private boolean indexed;

    /**
     * Get the name of this field's type. For example {@code int} for primitive integer type.
     */
    @NotNull(message = "simple fields must have a type")
    public String getType() {
        return this.type;
    }
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Get whether this field is indexed or not.
     */
    public boolean isIndexed() {
        return this.indexed;
    }
    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

// Object

    @Override
    public String toString() {
        return super.toString() + " of type " + this.type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleSchemaField that = (SimpleSchemaField)obj;
        return (this.type != null ? this.type.equals(that.type) : that.type == null) && this.indexed == that.indexed;
    }

    @Override
    public int hashCode() {
        return (this.type != null ? this.type.hashCode() : 0) ^ (this.indexed ? 1 : 0);
    }

// Cloneable

    @Override
    public SimpleSchemaField clone() {
        return (SimpleSchemaField)super.clone();
    }
}

