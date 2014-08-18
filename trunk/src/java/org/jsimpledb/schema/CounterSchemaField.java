
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;

/**
 * A counter field in a {@link SchemaObject}.
 */
public class CounterSchemaField extends SchemaField {

    @Override
    public void validate() {
        super.validate();
        if (this.getName() == null || this.getName().length() == 0)
            throw new InvalidSchemaException(this + " must specify a name");
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseCounterSchemaField(this);
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEmptyElement(COUNTER_FIELD_TAG.getNamespaceURI(), COUNTER_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer);
    }

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

