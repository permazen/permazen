
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.core.InvalidSchemaException;

/**
 * One object type in a {@link SchemaModel}.
 */
public class SchemaObject extends AbstractSchemaItem {

    private SortedMap<Integer, SchemaField> schemaFields = new TreeMap<>();

    public SortedMap<Integer, SchemaField> getSchemaFields() {
        return this.schemaFields;
    }
    public void setSchemaFields(SortedMap<Integer, SchemaField> schemaFields) {
        this.schemaFields = schemaFields;
    }

    @Override
    public void validate() {
        super.validate();
        if (this.getName() == null || this.getName().length() == 0)
            throw new InvalidSchemaException(this + " must have a name");
        for (SchemaField field : this.schemaFields.values()) {
            if (field.getName() == null || field.getName().length() == 0)
                throw new InvalidSchemaException(field + " of " + this + " must have a name");
            field.validate();
        }
    }

// Object

    @Override
    public String toString() {
        return "object " + super.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SchemaObject that = (SchemaObject)obj;
        return this.schemaFields.equals(that.schemaFields);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.schemaFields.hashCode();
    }

// Cloneable

    @Override
    public SchemaObject clone() {
        final SchemaObject clone = (SchemaObject)super.clone();
        clone.schemaFields = new TreeMap<>();
        for (SchemaField schemaField : this.schemaFields.values())
            clone.addSchemaField(schemaField.clone());
        return clone;
    }

// JiBX

    public void addSchemaField(SchemaField field) {
        this.schemaFields.put(field.getStorageId(), field);
    }

    public Iterator<SchemaField> iterateSchemaFields() {
        return this.schemaFields.values().iterator();
    }
}

