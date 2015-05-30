
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.jsimpledb.core.MapField;
import org.jsimpledb.util.DiffGenerating;
import org.jsimpledb.util.Diffs;

/**
 * A map field in one version of a {@link SchemaObjectType}.
 */
public class MapSchemaField extends ComplexSchemaField implements DiffGenerating<MapSchemaField> {

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
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.keyField = this.readSubField(reader, formatVersion, MapField.KEY_FIELD_NAME);
        this.valueField = this.readSubField(reader, formatVersion, MapField.VALUE_FIELD_NAME);
        this.expectClose(reader);
    }

    @Override
    QName getXMLTag() {
        return MAP_FIELD_TAG;
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseMapSchemaField(this);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(MapSchemaField that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        final Diffs keyDiffs = this.keyField.differencesFrom(that.keyField);
        if (!keyDiffs.isEmpty())
            diffs.add("changed key field", keyDiffs);
        final Diffs valueDiffs = this.valueField.differencesFrom(that.valueField);
        if (!valueDiffs.isEmpty())
            diffs.add("changed value field", valueDiffs);
        return diffs;
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

