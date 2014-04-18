
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

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

    @Override
    void readSubElements(XMLStreamReader reader) throws XMLStreamException {
        while (this.expect(reader, true, COUNTER_FIELD_TAG, LIST_FIELD_TAG, MAP_FIELD_TAG,
          REFERENCE_FIELD_TAG, SET_FIELD_TAG, SIMPLE_FIELD_TAG)) {
            SchemaField field;
            if (reader.getName().equals(COUNTER_FIELD_TAG))
                field = new CounterSchemaField();
            else if (reader.getName().equals(LIST_FIELD_TAG))
                field = new ListSchemaField();
            else if (reader.getName().equals(MAP_FIELD_TAG))
                field = new MapSchemaField();
            else if (reader.getName().equals(REFERENCE_FIELD_TAG))
                field = new ReferenceSchemaField();
            else if (reader.getName().equals(SET_FIELD_TAG))
                field = new SetSchemaField();
            else if (reader.getName().equals(SIMPLE_FIELD_TAG))
                field = new SimpleSchemaField();
            else
                throw new RuntimeException("internal error");
            field.readXML(reader);
            final int storageId = field.getStorageId();
            final SchemaField previous = this.schemaFields.put(storageId, field);
            if (previous != null) {
                throw new InvalidSchemaException("duplicate use of storage ID " + storageId
                  + " for both " + previous + " and " + field + " in " + this);
            }
        }
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        if (this.schemaFields.isEmpty())
            writer.writeEmptyElement(OBJECT_TAG.getNamespaceURI(), OBJECT_TAG.getLocalPart());
        else
            writer.writeStartElement(OBJECT_TAG.getNamespaceURI(), OBJECT_TAG.getLocalPart());
        this.writeAttributes(writer);
        for (SchemaField schemaField : this.schemaFields.values())
            schemaField.writeXML(writer);
        if (!this.schemaFields.isEmpty())
            writer.writeEndElement();
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
            clone.getSchemaFields().put(schemaField.getStorageId(), schemaField.clone());
        return clone;
    }
}

