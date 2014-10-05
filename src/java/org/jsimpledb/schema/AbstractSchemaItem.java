
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.util.AbstractXMLStreaming;

/**
 * Common superclass for {@link SchemaObject} and {@link SchemaField}.
 */
public abstract class AbstractSchemaItem extends AbstractXMLStreaming implements XMLConstants, Cloneable {

    private String name;
    private int storageId;

    /**
     * Get the name associated with this instance, if any.
     */
    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the storage ID associated with this instance.
     * Storage IDs must be positive values.
     */
    public int getStorageId() {
        return this.storageId;
    }
    public void setStorageId(int storageId) {
        this.storageId = storageId;
    }

    /**
     * Validate this instance.
     *
     * @throws InvalidSchemaException if this instance in invalid
     */
    public void validate() {
        if (name == null)
            throw new InvalidSchemaException(this + " must specify a name");
        if (!name.matches(SchemaItem.NAME_PATTERN))
            throw new InvalidSchemaException(this + " has an invalid name `" + name + "'");
        if (this.storageId <= 0)
            throw new InvalidSchemaException(this + " has an invalid storage ID " + this.storageId + "; must be greater than zero");
    }

    /**
     * Determine whether this instance is compatible with the given instance for use with the core API.
     * Two instances are compatible if they are identical in all respects except for object and field names
     * (to also include object and field names in the comparison, use {@link #equals equals()}).
     * The core API uses storage IDs, not names, to identify objects and fields.
     *
     * @param that other schema object
     * @throws IllegalArgumentException if {@code that} is null
     */
    public final boolean isCompatibleWith(AbstractSchemaItem that) {
        if (that == null)
            throw new IllegalArgumentException("null that");
        if (this.storageId != that.storageId)
            return false;
        if (this.getClass() != that.getClass())
            return false;
        return this.isCompatibleWithInternal(that);
    }

    abstract boolean isCompatibleWithInternal(AbstractSchemaItem that);

    void readXML(XMLStreamReader reader) throws XMLStreamException {
        this.readAttributes(reader);
        this.readSubElements(reader);
    }

    void readAttributes(XMLStreamReader reader) throws XMLStreamException {
        final String text = reader.getAttributeValue(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart());
        if (text != null) {
            try {
                this.setStorageId(Integer.valueOf(text));
            } catch (NumberFormatException e) {
                throw new XMLStreamException("invalid storage ID `" + text + "': must be a positive integer", reader.getLocation());
            }
        }
        final String newName = reader.getAttributeValue(NAME_ATTRIBUTE.getNamespaceURI(), NAME_ATTRIBUTE.getLocalPart());
        if (newName != null)
            this.setName(newName);
    }

    void readSubElements(XMLStreamReader reader) throws XMLStreamException {
        this.expect(reader, true);
    }

    abstract void writeXML(XMLStreamWriter writer) throws XMLStreamException;

    final void writeAttributes(XMLStreamWriter writer) throws XMLStreamException {
        this.writeAttributes(writer, true);
    }

    void writeAttributes(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeAttribute(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + this.storageId);
        if (includeName && this.name != null)
            writer.writeAttribute(NAME_ATTRIBUTE.getNamespaceURI(), NAME_ATTRIBUTE.getLocalPart(), this.name);
    }

// Object

    @Override
    public String toString() {
        return "#" + this.storageId + (this.name != null ? " `" + this.name + "'" : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractSchemaItem that = (AbstractSchemaItem)obj;
        return (this.name != null ? this.name.equals(that.name) : that.name == null) && this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return (this.name != null ? this.name.hashCode() : 0) ^ this.storageId;
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    public AbstractSchemaItem clone() {
        try {
            return (AbstractSchemaItem)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}

