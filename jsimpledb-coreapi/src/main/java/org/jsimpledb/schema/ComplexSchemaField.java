
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
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
    void validate() {
        super.validate();
        for (Map.Entry<String, SimpleSchemaField> entry : this.getSubFields().entrySet()) {
            final String subFieldName = entry.getKey();
            final SimpleSchemaField subField = entry.getValue();
            if (subField == null)
                throw new InvalidSchemaException("invalid " + this + ": missing sub-field `" + subFieldName + "'");
            subField.validate();
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
        if (!AbstractSchemaItem.allAreCompatible(this.getSubFields(), that.getSubFields()))
            return false;
        return true;
    }

    SimpleSchemaField readSubField(XMLStreamReader reader, int formatVersion, String name) throws XMLStreamException {
        final SimpleSchemaField field = this.readMappedType(reader, false, SchemaModel.SIMPLE_FIELD_TAG_MAP);
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

