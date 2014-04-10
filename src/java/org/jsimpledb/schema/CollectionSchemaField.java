
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Collections;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.jsimpledb.CollectionField;

/**
 * A collection field in one version of a {@link SchemaObject}.
 */
public abstract class CollectionSchemaField extends ComplexSchemaField {

    private SimpleSchemaField elementField;

    @NotNull(message = "collection fields must have an element sub-field")
    @Valid
    public SimpleSchemaField getElementField() {
        return this.elementField;
    }
    public void setElementField(SimpleSchemaField elementField) {
        this.elementField = elementField;
    }

    @Override
    public Map<String, SimpleSchemaField> getSubFields() {
        return Collections.<String, SimpleSchemaField> singletonMap(CollectionField.ELEMENT_FIELD_NAME, this.elementField);
    }

// Object

    @Override
    public String toString() {
        return super.toString() + " with element " + this.getElementField();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CollectionSchemaField that = (CollectionSchemaField)obj;
        return this.elementField != null ? this.elementField.equals(that.elementField) : that.elementField == null;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.elementField != null ? this.elementField.hashCode() : 0);
    }

// Cloneable

    @Override
    public CollectionSchemaField clone() {
        final CollectionSchemaField clone = (CollectionSchemaField)super.clone();
        clone.elementField = this.elementField.clone();
        return clone;
    }
}

