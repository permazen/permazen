
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.validation.ConstraintValidatorContext;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.dellroad.stuff.validation.SelfValidates;
import org.dellroad.stuff.validation.SelfValidating;
import org.dellroad.stuff.validation.SelfValidationException;

/**
 * One object type in a {@link SchemaModel}.
 */
@SelfValidates
public class SchemaObject extends AbstractSchemaItem implements SelfValidating {

    private SortedMap<Integer, SchemaField> schemaFields = new TreeMap<>();

    // Overridden for @NotNull annotation
    @NotNull(message = "must have a name")
    public String getName() {
        return super.getName();
    }

    @NotNull
    @Valid
    public SortedMap<Integer, SchemaField> getSchemaFields() {
        return this.schemaFields;
    }
    public void setSchemaFields(SortedMap<Integer, SchemaField> schemaFields) {
        this.schemaFields = schemaFields;
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

// SelfValidating

    @Override
    public void checkValid(ConstraintValidatorContext context) throws SelfValidationException {
        for (SchemaField field : this.getSchemaFields().values()) {
            if (field.getName() == null)
                throw new SelfValidationException(field + " must have a name");
            if (field instanceof ComplexSchemaField) {
                final ComplexSchemaField complexField = (ComplexSchemaField)field;
                for (SimpleSchemaField subField : complexField.getSubFields().values()) {
                    if (subField.getName() != null)
                        throw new SelfValidationException(subField + " must not specify a name");
                }
            }
        }
    }

// JiBX

    public void addSchemaField(SchemaField field) {
        this.schemaFields.put(field.getStorageId(), field);
    }

    public Iterator<SchemaField> iterateSchemaFields() {
        return this.schemaFields.values().iterator();
    }
}

