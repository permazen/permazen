
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.validation.constraints.NotNull;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.ReferenceType;

/**
 * A reference field in a {@link SchemaObject}.
 */
public class ReferenceSchemaField extends SimpleSchemaField {

    private DeleteAction onDelete;

    public ReferenceSchemaField() {
        this.setType(ReferenceType.NAME);
        this.setIndexed(true);
    }

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     */
    @NotNull(message = "a non-null onDelete value must be specified")
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }
    public void setOnDelete(DeleteAction onDelete) {
        this.onDelete = onDelete;
    }

    @Override
    public void setType(String type) {
        if (!ReferenceType.NAME.equals(type))
            throw new IllegalArgumentException("reference fields always have type `" + ReferenceType.NAME + "'");
        super.setType(type);
    }

    @Override
    public void setIndexed(boolean indexed) {
        if (!indexed)
            throw new IllegalArgumentException("reference fields are always indexed");
        super.setIndexed(indexed);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceSchemaField that = (ReferenceSchemaField)obj;
        return this.onDelete == that.onDelete;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.onDelete != null ? this.onDelete.hashCode() : 0);
    }

// Cloneable

    @Override
    public ReferenceSchemaField clone() {
        return (ReferenceSchemaField)super.clone();
    }
}

