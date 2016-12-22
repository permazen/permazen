
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * A counter field in a {@link SchemaObjectType}.
 */
public class CounterSchemaField extends SchemaField {

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseCounterSchemaField(this);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEmptyElement(COUNTER_FIELD_TAG.getNamespaceURI(), COUNTER_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer);
    }

// Compatibility

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that) {
        return true;
    }

// Object

    @Override
    public String toString() {
        return "counter " + super.toString();
    }

// Cloneable

    @Override
    public CounterSchemaField clone() {
        return (CounterSchemaField)super.clone();
    }
}

