
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.collect.ImmutableSortedMap;

import io.permazen.core.CollectionField;
import io.permazen.util.Diffs;

import java.util.NavigableMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * A collection field in one version of a {@link SchemaObjectType}.
 */
public abstract class CollectionSchemaField extends ComplexSchemaField {

    private SimpleSchemaField elementField;

// Properties

    /**
     * Get this collection field's element sub-field.
     *
     * @return element sub-field
     */
    public SimpleSchemaField getElementField() {
        return this.elementField;
    }

    /**
     * Set this collection field's element sub-field.
     *
     * @param elementField element sub-field
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setElementField(SimpleSchemaField elementField) {
        this.verifyNotLockedDown(false);
        this.elementField = elementField;
    }

// ComplexSchemaField

    @Override
    public final NavigableMap<String, SimpleSchemaField> getSubFields() {
        return ImmutableSortedMap.of(CollectionField.ELEMENT_FIELD_NAME, this.elementField);
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.elementField = this.readSubField(reader, formatVersion, CollectionField.ELEMENT_FIELD_NAME);
        this.expectClose(reader);
    }

// DiffGenerating

    protected Diffs differencesFrom(CollectionSchemaField that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        final Diffs elementDiffs = this.elementField.differencesFrom(that.elementField);
        if (!elementDiffs.isEmpty())
            diffs.add("changed element field", elementDiffs);
        return diffs;
    }

// Cloneable

    @Override
    public CollectionSchemaField clone() {
        final CollectionSchemaField clone = (CollectionSchemaField)super.clone();
        if (clone.elementField != null) {
            clone.elementField = clone.elementField.clone();
            clone.elementField.setParent(clone);
        }
        return clone;
    }
}
