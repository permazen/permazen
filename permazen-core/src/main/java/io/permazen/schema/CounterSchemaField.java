
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * A counter field in a {@link SchemaObjectType}.
 */
public class CounterSchemaField extends SchemaField {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.COUNTER_FIELD;

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseCounterSchemaField(this);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeStorageIds, boolean prettyPrint) throws XMLStreamException {
        this.writeEmptyItemElement(writer);
        this.writeAttributes(writer, includeStorageIds);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
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
