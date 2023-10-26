
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.MapField;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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
        this.verifyNotLockedDown();
        this.keyField = keyField;
    }

    public SimpleSchemaField getValueField() {
        return this.valueField;
    }
    public void setValueField(SimpleSchemaField valueField) {
        this.verifyNotLockedDown();
        this.valueField = valueField;
    }

    @Override
    public Map<String, SimpleSchemaField> getSubFields() {
        final LinkedHashMap<String, SimpleSchemaField> map = new LinkedHashMap<>(2);
        map.put(MapField.KEY_FIELD_NAME, this.keyField);
        map.put(MapField.VALUE_FIELD_NAME, this.valueField);
        return map;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        if (this.keyField != null)
            this.keyField.lockDown();
        if (this.valueField != null)
            this.valueField.lockDown();
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseMapSchemaField(this);
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.keyField = this.readSubField(reader, formatVersion, MapField.KEY_FIELD_NAME);
        this.valueField = this.readSubField(reader, formatVersion, MapField.VALUE_FIELD_NAME);
        this.expectClose(reader);
    }

// XML Writing

    @Override
    QName getXMLTag() {
        return XMLConstants.MAP_FIELD_TAG;
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
        return Objects.equals(this.keyField, that.keyField)
          && Objects.equals(this.valueField, that.valueField);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ Objects.hashCode(this.keyField)
          ^ Objects.hashCode(this.valueField);
    }

// Cloneable

    @Override
    public MapSchemaField clone() {
        final MapSchemaField clone = (MapSchemaField)super.clone();
        if (clone.keyField != null)
            clone.keyField = clone.keyField.clone();
        if (clone.valueField != null)
            clone.valueField = clone.valueField.clone();
        return clone;
    }
}
