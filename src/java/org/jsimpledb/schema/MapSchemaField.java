
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.HashMap;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.jsimpledb.core.MapField;

/**
 * A map field in one version of a {@link SchemaObject}.
 */
public class MapSchemaField extends ComplexSchemaField {

    private SimpleSchemaField keyField;
    private SimpleSchemaField valueField;

    @NotNull(message = "map fields must have key and value sub-fields")
    @Valid
    public SimpleSchemaField getKeyField() {
        return this.keyField;
    }
    public void setKeyField(SimpleSchemaField keyField) {
        this.keyField = keyField;
    }

    @NotNull(message = "map fields must have key and value sub-fields")
    @Valid
    public SimpleSchemaField getValueField() {
        return this.valueField;
    }
    public void setValueField(SimpleSchemaField valueField) {
        this.valueField = valueField;
    }

    @Override
    public Map<String, SimpleSchemaField> getSubFields() {
        final HashMap<String, SimpleSchemaField> map = new HashMap<String, SimpleSchemaField>(2);
        map.put(MapField.KEY_FIELD_NAME, this.keyField);
        map.put(MapField.VALUE_FIELD_NAME, this.valueField);
        return map;
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseMapSchemaField(this);
    }

// Object

    @Override
    public String toString() {
        return "map " + super.toString() + " with key " + this.getKeyField() + " and value " + this.getValueField();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapSchemaField that = (MapSchemaField)obj;
        return (this.keyField != null ? this.keyField.equals(that.keyField) : that.keyField == null)
          && (this.valueField != null ? this.valueField.equals(that.valueField) : that.valueField == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.keyField != null ? this.keyField.hashCode() : 0)
          ^ (this.valueField != null ? this.valueField.hashCode() : 0);
    }

// Cloneable

    @Override
    public MapSchemaField clone() {
        return (MapSchemaField)super.clone();
    }
}

