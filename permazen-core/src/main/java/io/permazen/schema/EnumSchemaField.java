
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

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.ENUM_FIELD;

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseEnumSchemaField(this);
    }

// XML Writing

    @Override
    void writeElement(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        this.writeStartElement(writer, XMLConstants.ENUM_FIELD_TAG);
    }

// Cloneable

    @Override
    public EnumSchemaField clone() {
        return (EnumSchemaField)super.clone();
    }
}
