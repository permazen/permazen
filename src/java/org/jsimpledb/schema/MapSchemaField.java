
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.jsimpledb.core.MapField;

/**
 * A map field in one version of a {@link SchemaObject}.
 */
public class MapSchemaField extends ComplexSchemaField {

    private SimpleSchemaField keyField;
    private SimpleSchemaField valueField;

    public SimpleSchemaField getKeyField() {
        return this.keyField;
    }
    public void setKeyField(SimpleSchemaField keyField) {
        this.keyField = keyField;
    }

    public SimpleSchemaField getValueField() {
        return this.valueField;
    }
    public void setValueField(SimpleSchemaField valueField) {
        this.valueField = valueField;
    }

    @Override
    public Map<String, SimpleSchemaField> getSubFields() {
        final LinkedHashMap<String, SimpleSchemaField> map = new LinkedHashMap<String, SimpleSchemaField>(2);
        map.put(MapField.KEY_FIELD_NAME, this.keyField);
        map.put(MapField.VALUE_FIELD_NAME, this.valueField);
        return map;
    }

    @Override
    void readSubElements(XMLStreamReader reader) throws XMLStreamException {
        this.keyField = this.readSubField(reader, MapField.KEY_FIELD_NAME);
        this.valueField = this.readSubField(reader, MapField.VALUE_FIELD_NAME);
        this.expect(reader, true);
    }

    @Override
    QName getXMLTag() {
        return MAP_FIELD_TAG;
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseMapSchemaField(this);
    }

// Object

    @Override
    public String toString() {
        return "map " + super.toString() + " with key " + this.keyField + " and value " + this.valueField;
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

