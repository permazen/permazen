
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.FieldType;
import io.permazen.core.InvalidSchemaException;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * A simple field in a {@link SchemaObjectType}.
 */
public class SimpleSchemaField extends SchemaField implements DiffGenerating<SimpleSchemaField> {

    private String type;
    private long encodingSignature;
    private boolean indexed;

    /**
     * Get the name of this field's type. For example {@code "int"} for primitive integer type,
     * {@code "java.util.Date"} for the built-in {@link java.util.Date} type, any custom type name, etc.
     *
     * @return field type name
     */
    public String getType() {
        return this.type;
    }
    public void setType(String type) {
        this.verifyNotLockedDown();
        this.type = type;
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

    /**
     * Get the encoding signature associated with this field's type.
     *
     * @return this field's encoding signature
     * @see io.permazen.core.FieldType
     */
    public long getEncodingSignature() {
        return this.encodingSignature;
    }
    public void setEncodingSignature(long encodingSignature) {
        this.verifyNotLockedDown();
        this.encodingSignature = encodingSignature;
    }

// Validation

    @Override
    void validate() {
        super.validate();
        this.validateType();
    }

    void validateType() {
        if (this.type == null)
            throw new InvalidSchemaException("invalid " + this + ": no type specified");
        if (!Pattern.compile(FieldType.NAME_PATTERN).matcher(this.type).matches()) {
            throw new InvalidSchemaException("invalid " + super.toString() + " type \"" + this.type
              + "\": does not match pattern \"" + FieldType.NAME_PATTERN + "\"");
        }
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
          && this.encodingSignature == that.encodingSignature
          && this.indexed == that.indexed;
    }

    boolean isCompatibleType(SimpleSchemaField that) {
        return Objects.equals(this.type, that.type);
    }

    @Override
    final void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        super.writeCompatibilityHashData(output);
        this.writeFieldTypeCompatibilityHashData(output);
        output.writeLong(this.encodingSignature);
        output.writeBoolean(this.indexed);
    }

    void writeFieldTypeCompatibilityHashData(DataOutputStream output) throws IOException {
        output.writeUTF(this.type);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        this.addTypeDifference(diffs, that);
        if (this.encodingSignature != that.encodingSignature)
            diffs.add("changed field type encoding signature from " + that.encodingSignature + " to " + this.encodingSignature);
        if (this.indexed != that.indexed)
            diffs.add((this.indexed ? "added" : "removed") + " index on field");
        return diffs;
    }

    void addTypeDifference(Diffs diffs, SimpleSchemaField that) {
        if (!Objects.equals(this.type, that.type))
            diffs.add("changed field type from \"" + that.type + "\" to \"" + this.type + "\"");
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        super.readAttributes(reader, formatVersion);
        final String typeAttr = this.getAttr(reader, XMLConstants.TYPE_ATTRIBUTE, false);
        if (typeAttr != null)
            this.setType(typeAttr);
        final Boolean indexedAttr = this.getBooleanAttr(reader, XMLConstants.INDEXED_ATTRIBUTE, false);
        if (indexedAttr != null)
            this.setIndexed(indexedAttr);
        final Long encodingSignatureAttr = this.getLongAttr(reader, XMLConstants.ENCODING_SIGNATURE_ATTRIBUTE, false);
        if (encodingSignatureAttr != null)
            this.setEncodingSignature(encodingSignatureAttr);
    }

    @Override
    final void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        this.writeXML(writer, true);
    }

// XML Writing

    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeEmptyElement(XMLConstants.SIMPLE_FIELD_TAG.getNamespaceURI(), XMLConstants.SIMPLE_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
    }

    @Override
    final void writeAttributes(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        super.writeAttributes(writer, includeName);
        this.writeSimpleAttributes(writer);
        if (this.encodingSignature != 0) {
            writer.writeAttribute(XMLConstants.ENCODING_SIGNATURE_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.ENCODING_SIGNATURE_ATTRIBUTE.getLocalPart(), "" + this.encodingSignature);
        }
    }

    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        this.writeTypeAttribute(writer);
        if (this.indexed) {
            writer.writeAttribute(XMLConstants.INDEXED_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.INDEXED_ATTRIBUTE.getLocalPart(), "" + this.indexed);
        }
    }

    void writeTypeAttribute(XMLStreamWriter writer) throws XMLStreamException {
        if (this.type != null) {
            writer.writeAttribute(XMLConstants.TYPE_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.TYPE_ATTRIBUTE.getLocalPart(), this.type);
        }
    }

// Object

    @Override
    public String toString() {
        return super.toString()
          + (this.type != null ? " of type " + this.type : "")
          + (this.encodingSignature != 0 ? " (encoding " + this.encodingSignature + ")" : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleSchemaField that = (SimpleSchemaField)obj;
        return Objects.equals(this.type, that.type)
          && this.encodingSignature == that.encodingSignature
          && this.indexed == that.indexed;
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ Objects.hashCode(this.type)
          ^ ((Long)this.encodingSignature).hashCode()
          ^ (this.indexed ? 1 : 0);
    }

// Cloneable

    @Override
    public SimpleSchemaField clone() {
        return (SimpleSchemaField)super.clone();
    }
}

