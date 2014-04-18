
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
 * A complex field in one version of a {@link SchemaObject}.
 */
public abstract class ComplexSchemaField extends SchemaField {

    @Override
    public void validate() {
        super.validate();
        if (this.getName() == null || this.getName().length() == 0)
            throw new InvalidSchemaException(this + " must specify a name");
        for (Map.Entry<String, SimpleSchemaField> entry : this.getSubFields().entrySet()) {
            final String subFieldName = entry.getKey();
            final SimpleSchemaField subField = entry.getValue();
            if (subField == null)
                throw new InvalidSchemaException("invalid " + this + ": missing sub-field `" + subFieldName + "'");
            if (subField.getName() != null)
                throw new InvalidSchemaException("sub-" + subField + " of " + this + " must not specify a name");
            subField.validate();
        }
    }

    public abstract Map<String, SimpleSchemaField> getSubFields();

    SimpleSchemaField readSubField(XMLStreamReader reader) throws XMLStreamException {
        this.expect(reader, false, REFERENCE_FIELD_TAG, SIMPLE_FIELD_TAG);
        SimpleSchemaField field;
        if (reader.getName().equals(REFERENCE_FIELD_TAG))
            field = new ReferenceSchemaField();
        else if (reader.getName().equals(SIMPLE_FIELD_TAG))
            field = new SimpleSchemaField();
        else
            throw new RuntimeException("internal error");
        field.readXML(reader);
        return field;
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        final QName tag = this.getXMLTag();
        writer.writeStartElement(tag.getNamespaceURI(), tag.getLocalPart());
        this.writeAttributes(writer);
        for (SimpleSchemaField subField : this.getSubFields().values())
            subField.writeXML(writer);
        writer.writeEndElement();
    }

    abstract QName getXMLTag();

// Cloneable

    @Override
    public ComplexSchemaField clone() {
        return (ComplexSchemaField)super.clone();
    }
}

