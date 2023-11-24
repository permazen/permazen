
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.encoding.Encoding;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * An enum array field (of one or more dimensions) in a {@link SchemaObjectType}.
 */
public class EnumArraySchemaField extends AbstractEnumSchemaField {

    private int dimensions;

    /**
     * Get the number of enum array dimensions.
     *
     * @return number of dimensions, a value from 1 to 255
     */
    public int getDimensions() {
        return this.dimensions;
    }
    public void setDimensions(final int dimensions) {
        this.verifyNotLockedDown();
        this.dimensions = dimensions;
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseEnumArraySchemaField(this);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.dimensions < 1 || this.dimensions > Encoding.MAX_ARRAY_DIMENSIONS) {
            throw new IllegalArgumentException("invalid " + this + ": number of dimensions ("
              + this.dimensions + ") must be in the range 1 to " + Encoding.MAX_ARRAY_DIMENSIONS);
        }
    }

// Compatibility

    @Override
    boolean isCompatibleType(SimpleSchemaField field) {
        if (!super.isCompatibleType(field))
            return false;
        final EnumArraySchemaField that = (EnumArraySchemaField)field;
        return this.dimensions == that.dimensions;
    }

    @Override
    void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        super.writeCompatibilityHashData(output);
        output.writeInt(this.dimensions);
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        super.readAttributes(reader, formatVersion);
        this.dimensions = this.getIntAttr(reader, XMLConstants.DIMENSIONS_ATTRIBUTE);
        if (this.dimensions < 1 || this.dimensions > Encoding.MAX_ARRAY_DIMENSIONS) {
            throw this.newInvalidAttributeException(reader, XMLConstants.DIMENSIONS_ATTRIBUTE,
              "number of dimensions must be in the range 1 to " + Encoding.MAX_ARRAY_DIMENSIONS);
        }
    }

// XML Writing

    @Override
    void writeElement(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeStartElement(XMLConstants.ENUM_ARRAY_FIELD_TAG.getNamespaceURI(),
          XMLConstants.ENUM_ARRAY_FIELD_TAG.getLocalPart());
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeAttribute(XMLConstants.DIMENSIONS_ATTRIBUTE.getNamespaceURI(),
          XMLConstants.DIMENSIONS_ATTRIBUTE.getLocalPart(), "" + this.dimensions);
        super.writeSimpleAttributes(writer);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof EnumArraySchemaField)) {
            diffs.add("change type from " + other.getClass().getSimpleName() + " to " + this.getClass().getSimpleName());
            return diffs;
        }
        final EnumArraySchemaField that = (EnumArraySchemaField)other;
        if (this.dimensions != that.dimensions)
            diffs.add("changed number of dimensions from " + that.dimensions + " to " + this.dimensions);
        return diffs;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final EnumArraySchemaField that = (EnumArraySchemaField)obj;
        return this.dimensions == that.dimensions;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.dimensions;
    }

// Cloneable

    @Override
    public EnumArraySchemaField clone() {
        return (EnumArraySchemaField)super.clone();
    }
}
