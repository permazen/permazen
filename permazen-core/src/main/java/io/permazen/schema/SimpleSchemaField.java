
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.InvalidSchemaException;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingIds;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * A simple field in a {@link SchemaObjectType}.
 */
public class SimpleSchemaField extends SchemaField implements DiffGenerating<SimpleSchemaField> {

    private EncodingId encodingId;
    private boolean indexed;

    @SuppressWarnings("this-escape")
    public SimpleSchemaField() {
        if (this.isAlwaysIndexed())
            this.setIndexed(true);
    }

    /**
     * Get the {@link EncodingId} that identifies how this field's values are encoded.
     *
     * @return field encoding ID
     */
    public EncodingId getEncodingId() {
        return this.encodingId;
    }
    public void setEncodingId(EncodingId encodingId) {
        this.verifyNotLockedDown();
        this.encodingId = encodingId;
    }

    /**
     * Get whether this field is indexed or not.
     *
     * @return true if this field is indexed
     */
    public boolean isIndexed() {
        return this.indexed;
    }
    public void setIndexed(boolean indexed) {
        this.verifyNotLockedDown();
        this.indexed = indexed;
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (!this.hasFixedEncoding() && this.encodingId == null)
            throw new InvalidSchemaException("invalid " + this + ": no encoding ID specified");
        else if (this.hasFixedEncoding() && this.encodingId != null)
            throw new InvalidSchemaException("invalid " + this + ": encoding ID should be null");
        if (this.isAlwaysIndexed() && !this.isIndexed())
            throw new IllegalArgumentException("invalid " + this + ": field must always be indexed");
    }

    boolean hasFixedEncoding() {
        return false;
    }

    boolean isAlwaysIndexed() {
        return false;
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseSimpleSchemaField(this);
    }

// Compatibility

    @Override
    final boolean isCompatibleWith(SchemaField field) {
        if (field.getClass() != this.getClass())
            return false;
        final SimpleSchemaField that = (SimpleSchemaField)field;
        return this.isCompatibleType(that)
          && this.indexed == that.indexed;
    }

    boolean isCompatibleType(SimpleSchemaField that) {
        return Objects.equals(this.encodingId, that.encodingId);
    }

    @Override
    void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        super.writeCompatibilityHashData(output);
        output.writeBoolean(this.encodingId != null);
        if (this.encodingId != null)
            output.writeUTF(this.encodingId.getId());
        output.writeBoolean(this.indexed);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        if (!this.hasFixedEncoding() && !Objects.equals(this.encodingId, that.encodingId))
            diffs.add("changed field encoding ID from \"" + that.encodingId + "\" to \"" + this.encodingId + "\"");
        if (this.indexed != that.indexed)
            diffs.add((this.indexed ? "added" : "removed") + " index on field");
        return diffs;
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        super.readAttributes(reader, formatVersion);
        final String encodingAttr = this.getAttr(reader, XMLConstants.ENCODING_ATTRIBUTE, false);
        if (encodingAttr != null) {
            try {
                this.setEncodingId(new EncodingId(encodingAttr));
            } catch (IllegalArgumentException e) {
                throw this.newInvalidAttributeException(reader, XMLConstants.ENCODING_ATTRIBUTE,
                  "invalid encoding ID \"" + encodingAttr + "\"");
            }
        }
        final Boolean indexedAttr = this.getBooleanAttr(reader, XMLConstants.INDEXED_ATTRIBUTE, false);
        if (indexedAttr != null)
            this.setIndexed(indexedAttr);
    }

// XML Writing

    @Override
    final void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        this.writeXML(writer, true);
    }

    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeEmptyElement(XMLConstants.SIMPLE_FIELD_TAG.getNamespaceURI(), XMLConstants.SIMPLE_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
    }

    @Override
    final void writeAttributes(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        super.writeAttributes(writer, includeName);
        this.writeSimpleAttributes(writer);
    }

    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        if (this.encodingId != null) {
            writer.writeAttribute(XMLConstants.ENCODING_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.ENCODING_ATTRIBUTE.getLocalPart(), this.encodingId.getId());
        }
        if (!this.isAlwaysIndexed() && this.indexed) {
            writer.writeAttribute(XMLConstants.INDEXED_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.INDEXED_ATTRIBUTE.getLocalPart(), "" + this.indexed);
        }
    }

// Object

    @Override
    public String toString() {
        String string = super.toString();
        if (this.encodingId != null) {
            final String id = this.encodingId.getId();
            string += id.startsWith(EncodingIds.PERMAZEN_PREFIX) ?
              " type " + id.substring(EncodingIds.PERMAZEN_PREFIX.length()) :
              " type \"" + id + "\"";
        }
        return string;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleSchemaField that = (SimpleSchemaField)obj;
        return Objects.equals(this.encodingId, that.encodingId)
          && this.indexed == that.indexed;
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ Objects.hashCode(this.encodingId)
          ^ Boolean.hashCode(this.indexed);
    }

// Cloneable

    @Override
    public SimpleSchemaField clone() {
        return (SimpleSchemaField)super.clone();
    }
}
