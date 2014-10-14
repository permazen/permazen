
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;

/**
 * A complex field in one version of a {@link SchemaObjectType}.
 */
public abstract class ComplexSchemaField extends SchemaField {

    @Override
    public void validate() {
        super.validate();
        for (Map.Entry<String, SimpleSchemaField> entry : this.getSubFields().entrySet()) {
            final String subFieldName = entry.getKey();
            final SimpleSchemaField subField = entry.getValue();
            subField.validate();
            if (subField == null)
                throw new InvalidSchemaException("invalid " + this + ": missing sub-field `" + subFieldName + "'");
            if (!subFieldName.equals(subField.getName())) {
                throw new InvalidSchemaException("sub-" + subField + " of " + this + " has the wrong name `"
                  + subField.getName() + "' != `" + subFieldName + "'");
            }
        }
    }

    public abstract Map<String, SimpleSchemaField> getSubFields();

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final ComplexSchemaField that = (ComplexSchemaField)that0;
        if (!this.getSubFields().keySet().equals(that.getSubFields().keySet()))
            return false;
        for (String subFieldName : this.getSubFields().keySet()) {
            final SimpleSchemaField thisSubField = this.getSubFields().get(subFieldName);
            final SimpleSchemaField thatSubField = that.getSubFields().get(subFieldName);
            if (!thisSubField.isCompatibleWith(thatSubField))
                return false;
        }
        return true;
    }

    SimpleSchemaField readSubField(XMLStreamReader reader, int formatVersion, String name) throws XMLStreamException {
        this.expect(reader, false, REFERENCE_FIELD_TAG, SIMPLE_FIELD_TAG);
        final SimpleSchemaField field;
        if (reader.getName().equals(REFERENCE_FIELD_TAG))
            field = new ReferenceSchemaField();
        else if (reader.getName().equals(SIMPLE_FIELD_TAG))
            field = new SimpleSchemaField();
        else
            throw new RuntimeException("internal error");
        field.readXML(reader, formatVersion);
        if (field.getName() == null)
            field.setName(name);
        return field;
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        final QName tag = this.getXMLTag();
        writer.writeStartElement(tag.getNamespaceURI(), tag.getLocalPart());
        this.writeAttributes(writer);
        for (SimpleSchemaField subField : this.getSubFields().values())
            subField.writeXML(writer, false);                               // omit (redundant) names for sub-fields
        writer.writeEndElement();
    }

    abstract QName getXMLTag();

// Cloneable

    @Override
    public ComplexSchemaField clone() {
        return (ComplexSchemaField)super.clone();
    }
}

