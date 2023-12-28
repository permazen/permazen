
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.collect.ImmutableSortedMap;

import io.permazen.core.MapField;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import java.util.NavigableMap;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * A map field in one version of a {@link SchemaObjectType}.
 */
public class MapSchemaField extends ComplexSchemaField implements DiffGenerating<MapSchemaField> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.MAP_FIELD;

    private SimpleSchemaField keyField;
    private SimpleSchemaField valueField;

// Properties

    /**
     * Get this map field's key sub-field.
     *
     * @return key sub-field
     */
    public SimpleSchemaField getKeyField() {
        return this.keyField;
    }

    /**
     * Set this map field's key sub-field.
     *
     * @param keyField key sub-field
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setKeyField(SimpleSchemaField keyField) {
        this.verifyNotLockedDown(false);
        this.keyField = keyField;
    }

    /**
     * Get this map field's value sub-field.
     *
     * @return value sub-field
     */
    public SimpleSchemaField getValueField() {
        return this.valueField;
    }

    /**
     * Set this map field's value sub-field.
     *
     * @param valueField value sub-field
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setValueField(SimpleSchemaField valueField) {
        this.verifyNotLockedDown(false);
        this.valueField = valueField;
    }

// ComplexSchemaField

    @Override
    public NavigableMap<String, SimpleSchemaField> getSubFields() {
        return ImmutableSortedMap.of(MapField.KEY_FIELD_NAME, this.keyField, MapField.VALUE_FIELD_NAME, this.valueField);
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
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
        return "map " + super.toString();
    }

// Cloneable

    @Override
    public MapSchemaField clone() {
        final MapSchemaField clone = (MapSchemaField)super.clone();
        if (clone.keyField != null) {
            clone.keyField = clone.keyField.clone();
            clone.keyField.setParent(clone);
        }
        if (clone.valueField != null) {
            clone.valueField = clone.valueField.clone();
            clone.valueField.setParent(clone);
        }
        return clone;
    }
}
