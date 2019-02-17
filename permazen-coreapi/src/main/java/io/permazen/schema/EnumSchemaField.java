
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * An enum field in a {@link SchemaObjectType}.
 */
public class EnumSchemaField extends AbstractEnumSchemaField {

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseEnumSchemaField(this);
    }

// XML Writing

    @Override
    void writeElement(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeStartElement(XMLConstants.ENUM_FIELD_TAG.getNamespaceURI(), XMLConstants.ENUM_FIELD_TAG.getLocalPart());
    }

// Cloneable

    @Override
    public EnumSchemaField clone() {
        return (EnumSchemaField)super.clone();
    }
}
