
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.InvalidSchemaException;

/**
 * A reference field in a {@link SchemaObjectType}.
 */
public class ReferenceSchemaField extends SimpleSchemaField {

    private DeleteAction onDelete;
    private SortedSet<Integer> objectTypes;

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

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return this.objectTypes;
    }
    public void setObjectTypes(SortedSet<Integer> objectTypes) {
        this.objectTypes = objectTypes;
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
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final ReferenceSchemaField that = (ReferenceSchemaField)that0;
        if (!super.isCompatibleWithInternal(that))
            return false;
        if (!this.onDelete.equals(that.onDelete))
            return false;
        if (!(this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null))
            return false;
        return true;
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        super.readAttributes(reader, formatVersion);
        final String text = this.getAttr(reader, ON_DELETE_ATTRIBUTE, false);
        final DeleteAction action;
        if (text != null) {
            if ((action = Enum.valueOf(DeleteAction.class, text)) == null) {
                throw new XMLStreamException("invalid value `" + text
                  + " for \"" + ON_DELETE_ATTRIBUTE.getLocalPart() + "\" attribute in " + this, reader.getLocation());
            }
        } else
            action = DeleteAction.EXCEPTION;
        this.setOnDelete(action);
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {

        // Any restrictions?
        if (!this.expect(reader, true, OBJECT_TYPES_TAG)) {
            this.objectTypes = null;
            return;
        }

        // Read list of zero or more permitted storage ID
        this.objectTypes = new TreeSet<>();
        while (this.expect(reader, true, OBJECT_TYPE_TAG)) {
            this.objectTypes.add(this.getIntAttr(reader, STORAGE_ID_ATTRIBUTE));
            this.expectClose(reader);           // </ObjectType>
        }

        // Read closing </ReferenceField>
        this.expectClose(reader);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        if (this.objectTypes != null)
            writer.writeStartElement(REFERENCE_FIELD_TAG.getNamespaceURI(), REFERENCE_FIELD_TAG.getLocalPart());
        else
            writer.writeEmptyElement(REFERENCE_FIELD_TAG.getNamespaceURI(), REFERENCE_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
        if (this.onDelete != null)
            writer.writeAttribute(ON_DELETE_ATTRIBUTE.getNamespaceURI(), ON_DELETE_ATTRIBUTE.getLocalPart(), this.onDelete.name());
        if (this.objectTypes != null) {
            writer.writeStartElement(OBJECT_TYPES_TAG.getNamespaceURI(), OBJECT_TYPES_TAG.getLocalPart());
            for (int storageId : this.objectTypes) {
                writer.writeEmptyElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
                writer.writeAttribute(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + storageId);
            }
            writer.writeEndElement();           // </ObjectTypes>
            writer.writeEndElement();           // </ReferenceField>
        }
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
        return this.onDelete == that.onDelete
          && (this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.onDelete != null ? this.onDelete.hashCode() : 0)
          ^ (this.objectTypes != null ? this.objectTypes.hashCode() : 0);
    }

// Cloneable

    @Override
    public ReferenceSchemaField clone() {
        final ReferenceSchemaField clone = (ReferenceSchemaField)super.clone();
        if (clone.objectTypes != null)
            clone.objectTypes = new TreeSet<Integer>(clone.objectTypes);
        return clone;
    }
}

