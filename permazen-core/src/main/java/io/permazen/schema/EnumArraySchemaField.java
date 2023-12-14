
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

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.ENUM_ARRAY_FIELD;

    private int dimensions;

// Properties

    /**
     * Get the number of enum array dimensions.
     *
     * @return number of dimensions, a value from 1 to 255
     */
    public int getDimensions() {
        return this.dimensions;
    }

    /**
     * Set the number of enum array dimensions.
     *
     * @param dimensions number of dimensions
     * @throws UnsupportedOperationException if this instance is locked down
     */
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
            throw new IllegalArgumentException(String.format(
              "invalid %s: number of dimensions (%d) must be in the range 1 to %d",
              this, this.dimensions, Encoding.MAX_ARRAY_DIMENSIONS));
        }
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        output.writeInt(this.dimensions);
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion, boolean requireName) throws XMLStreamException {
        super.readAttributes(reader, formatVersion, requireName);
        this.dimensions = this.getIntAttr(reader, XMLConstants.DIMENSIONS_ATTRIBUTE);
        if (this.dimensions < 1 || this.dimensions > Encoding.MAX_ARRAY_DIMENSIONS) {
            throw this.newInvalidAttributeException(reader, XMLConstants.DIMENSIONS_ATTRIBUTE,
              String.format("number of dimensions must be in the range 1 to %d", Encoding.MAX_ARRAY_DIMENSIONS));
        }
    }

// XML Writing

    @Override
    void writeElement(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        this.writeStartElement(writer, XMLConstants.ENUM_ARRAY_FIELD_TAG);
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        this.writeAttr(writer, XMLConstants.DIMENSIONS_ATTRIBUTE, this.dimensions);
        super.writeSimpleAttributes(writer);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof EnumArraySchemaField)) {
            diffs.add(String.format("changed type from %s to %s",
              other.getClass().getSimpleName(), this.getClass().getSimpleName()));
            return diffs;
        }
        final EnumArraySchemaField that = (EnumArraySchemaField)other;
        if (this.dimensions != that.dimensions)
            diffs.add(String.format("changed number of dimensions from %d to %d", that.dimensions, this.dimensions));
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
