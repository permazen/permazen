
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.InvalidSchemaException;

/**
 * A reference field in a {@link SchemaObject}.
 */
public class ReferenceSchemaField extends SimpleSchemaField {

    private DeleteAction onDelete;

    public ReferenceSchemaField() {
        this.setType(FieldType.REFERENCE_TYPE_NAME);
        this.setIndexed(true);
    }

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }
    public void setOnDelete(DeleteAction onDelete) {
        this.onDelete = onDelete;
    }

    @Override
    public void validate() {
        super.validate();
        if (!FieldType.REFERENCE_TYPE_NAME.equals(this.getType())) {
            throw new InvalidSchemaException("invalid " + this + ": reference fields must have type `"
              + FieldType.REFERENCE_TYPE_NAME + "'");
        }
        if (!this.isIndexed())
            throw new IllegalArgumentException("invalid " + this + ": reference fields must always be indexed");
        if (this.onDelete == null)
            throw new InvalidSchemaException("invalid " + this + ": no delete action specified");
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseReferenceSchemaField(this);
    }

    @Override
    void readAttributes(XMLStreamReader reader) throws XMLStreamException {
        super.readAttributes(reader);
        final String text = reader.getAttributeValue(ON_DELETE_ATTRIBUTE.getNamespaceURI(), ON_DELETE_ATTRIBUTE.getLocalPart());
        DeleteAction action = DeleteAction.EXCEPTION;
        if (text != null) {
            if ((action = Enum.valueOf(DeleteAction.class, text)) == null) {
                throw new XMLStreamException("invalid value `" + text
                  + " for \"" + ON_DELETE_ATTRIBUTE.getLocalPart() + "\" attribute in " + this);
            }
        }
        this.setOnDelete(action);
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEmptyElement(REFERENCE_FIELD_TAG.getNamespaceURI(), REFERENCE_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer);
        if (this.onDelete != null)
            writer.writeAttribute(ON_DELETE_ATTRIBUTE.getNamespaceURI(), ON_DELETE_ATTRIBUTE.getLocalPart(), this.onDelete.name());
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        // don't need to write type or indexed
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceSchemaField that = (ReferenceSchemaField)obj;
        return this.onDelete == that.onDelete;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.onDelete != null ? this.onDelete.hashCode() : 0);
    }

// Cloneable

    @Override
    public ReferenceSchemaField clone() {
        return (ReferenceSchemaField)super.clone();
    }
}

