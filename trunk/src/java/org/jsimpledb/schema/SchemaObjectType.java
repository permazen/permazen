
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
public class SchemaObjectType extends AbstractSchemaItem {

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
        final TreeMap<String, SchemaField> fieldsByName = new TreeMap<>();
        for (SchemaField field : this.schemaFields.values()) {
            field.validate();
            final String fieldName = field.getName();
            final SchemaField otherField = fieldsByName.put(fieldName, field);
            if (otherField != null)
                throw new InvalidSchemaException("duplicate field name `" + fieldName + "'");
        }
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        while (this.expect(reader, true, COUNTER_FIELD_TAG, ENUM_FIELD_TAG, LIST_FIELD_TAG, MAP_FIELD_TAG,
          REFERENCE_FIELD_TAG, SET_FIELD_TAG, SIMPLE_FIELD_TAG)) {
            SchemaField field;
            if (reader.getName().equals(COUNTER_FIELD_TAG))
                field = new CounterSchemaField();
            else if (reader.getName().equals(ENUM_FIELD_TAG))
                field = new EnumSchemaField();
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
            field.readXML(reader, formatVersion);
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
            writer.writeEmptyElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
        else
            writer.writeStartElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
        this.writeAttributes(writer);
        for (SchemaField schemaField : this.schemaFields.values())
            schemaField.writeXML(writer);
        if (!this.schemaFields.isEmpty())
            writer.writeEndElement();
    }

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final SchemaObjectType that = (SchemaObjectType)that0;
        if (!this.schemaFields.keySet().equals(that.schemaFields.keySet()))
            return false;
        for (int storageId : this.schemaFields.keySet()) {
            final SchemaField thisSchemaField = this.schemaFields.get(storageId);
            final SchemaField thatSchemaField = that.schemaFields.get(storageId);
            if (!thisSchemaField.isCompatibleWith(thatSchemaField))
                return false;
        }
        return true;
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
        final SchemaObjectType that = (SchemaObjectType)obj;
        return this.schemaFields.equals(that.schemaFields);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.schemaFields.hashCode();
    }

// Cloneable

    @Override
    public SchemaObjectType clone() {
        final SchemaObjectType clone = (SchemaObjectType)super.clone();
        clone.schemaFields = new TreeMap<>();
        for (SchemaField schemaField : this.schemaFields.values())
            clone.getSchemaFields().put(schemaField.getStorageId(), schemaField.clone());
        return clone;
    }
}

