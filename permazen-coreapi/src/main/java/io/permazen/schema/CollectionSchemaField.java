
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import io.permazen.core.CollectionField;
import io.permazen.util.Diffs;

/**
 * A collection field in one version of a {@link SchemaObjectType}.
 */
public abstract class CollectionSchemaField extends ComplexSchemaField {

    private SimpleSchemaField elementField;

    public SimpleSchemaField getElementField() {
        return this.elementField;
    }
    public void setElementField(SimpleSchemaField elementField) {
        this.verifyNotLockedDown();
        this.elementField = elementField;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        if (this.elementField != null)
            this.elementField.lockDown();
    }

// ComplexSchemaField

    @Override
    public Map<String, SimpleSchemaField> getSubFields() {
        return Collections.<String, SimpleSchemaField>singletonMap(CollectionField.ELEMENT_FIELD_NAME, this.elementField);
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

// Object

    @Override
    public String toString() {
        return super.toString() + (this.elementField != null ? " with element " + this.elementField : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CollectionSchemaField that = (CollectionSchemaField)obj;
        return Objects.equals(this.elementField, that.elementField);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.elementField);
    }

// Cloneable

    @Override
    public CollectionSchemaField clone() {
        final CollectionSchemaField clone = (CollectionSchemaField)super.clone();
        if (clone.elementField != null)
            clone.elementField = clone.elementField.clone();
        return clone;
    }
}

