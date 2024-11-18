
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
import java.util.Optional;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * A simple field in a {@link SchemaObjectType}.
 */
public class SimpleSchemaField extends SchemaField implements DiffGenerating<SimpleSchemaField> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.SIMPLE_FIELD;

    private ComplexSchemaField parent;
    private EncodingId encodingId;
    private boolean indexed;

    @SuppressWarnings("this-escape")
    public SimpleSchemaField() {
        if (this.isAlwaysIndexed())
            this.setIndexed(true);
    }

// Properties

    /**
     * Get the full name of this field.
     *
     * <p>
     * If the field is a sub-field of a complex field, the full name is the field's name qualified
     * by the parent field name, e.g., {@code "mymap.key"}. Otherwise, the full is is the same as the name.
     *
     * @return this field's full name
     */
    public String getFullName() {
        return Optional.ofNullable(this.parent)
          .map(ComplexSchemaField::getName)
          .map(parentName -> parentName + "." + this.getName())
          .orElse(this.getName());
    }

    /**
     * Get the parent field if this is a sub-field of a {@link ComplexSchemaField}.
     *
     * @return parent field, or null if this is not a sub-field
     */
    public ComplexSchemaField getParent() {
        return this.parent;
    }

    /**
     * Set the parent complex field.
     *
     * <p>
     * Note: this field is considered derived information, and will be set automatically
     * when a containing {@link SchemaObjectType} or {@link ComplexSchemaField} is locked down.
     *
     * @param parent complex field parent of this field, or null if this field is not a sub-field
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public final void setParent(ComplexSchemaField parent) {
        this.verifyNotLockedDown(false);
        this.parent = parent;
    }

    /**
     * Get the {@link EncodingId} that identifies how this field's values are encoded.
     *
     * @return field encoding ID
     */
    public EncodingId getEncodingId() {
        return this.encodingId;
    }

    /**
     * Set the {@link EncodingId} that identifies how this field's values are encoded.
     *
     * @param encodingId field encoding ID
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setEncodingId(EncodingId encodingId) {
        this.verifyNotLockedDown(false);
        this.encodingId = encodingId;
    }

    /**
     * Get whether this field is indexed.
     *
     * @return true if this field is indexed
     */
    public boolean isIndexed() {
        return this.indexed;
    }

    /**
     * Set whether this field is indexed.
     *
     * @param indexed true if this field is indexed, otherwise false
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setIndexed(boolean indexed) {
        this.verifyNotLockedDown(false);
        this.indexed = indexed;
    }

    /**
     * Determine if this field has a fixed encoding.
     *
     * @return true if this is a {@link ReferenceSchemaField} or {@link AbstractEnumSchemaField}, otherwise false
     */
    public boolean hasFixedEncoding() {
        return false;
    }

    /**
     * Determine if this field is always indexed.
     *
     * @return true if this is a {@link ReferenceSchemaField}, otherwise false
     */
    public boolean isAlwaysIndexed() {
        return false;
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (!this.hasFixedEncoding() && this.encodingId == null)
            throw new InvalidSchemaException(String.format("invalid %s: %s", this, "no encoding ID specified"));
        else if (this.hasFixedEncoding() && this.encodingId != null)
            throw new InvalidSchemaException(String.format("invalid %s: %s", this, "encoding ID should be null"));
        if (this.isAlwaysIndexed() && !this.isIndexed())
            throw new InvalidSchemaException(String.format("invalid %s: %s", this, "field must always be indexed"));
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseSimpleSchemaField(this);
    }

// Schema ID

    @Override
    public ItemType getItemType() {
        return ITEM_TYPE;
    }

    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        output.writeBoolean(this.parent != null);
        if (this.parent != null) {
            output.writeUTF(this.parent.getItemType().getTypeCode());
            output.writeUTF(this.parent.getName());
        }
        if (!this.hasFixedEncoding())
            output.writeUTF(this.encodingId.getId());
        output.writeBoolean(forSchemaModel);
        if (forSchemaModel)
            output.writeBoolean(this.indexed);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        if (!this.hasFixedEncoding() && !Objects.equals(this.encodingId, that.encodingId))
            diffs.add(String.format("changed field encoding ID from \"%s\" to \"%s\"", that.encodingId, this.encodingId));
        if (this.indexed != that.indexed)
            diffs.add((this.indexed ? "added" : "removed") + " index on field");
        return diffs;
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion, boolean requireName) throws XMLStreamException {
        super.readAttributes(reader, formatVersion, requireName);
        final String encodingAttr = this.getAttr(reader, XMLConstants.ENCODING_ATTRIBUTE, false);
        if (encodingAttr != null) {
            try {
                this.setEncodingId(new EncodingId(encodingAttr));
            } catch (IllegalArgumentException e) {
                throw this.newInvalidAttributeException(reader, XMLConstants.ENCODING_ATTRIBUTE,
                  String.format("invalid encoding ID \"%s\"", encodingAttr), e);
            }
        }
        final Boolean indexedAttr = this.getBooleanAttr(reader, XMLConstants.INDEXED_ATTRIBUTE, false);
        if (indexedAttr != null)
            this.setIndexed(indexedAttr);
    }

// XML Writing

    @Override
    final void writeXML(XMLStreamWriter writer, boolean includeStorageIds, boolean prettyPrint) throws XMLStreamException {
        this.writeXML(writer, includeStorageIds, prettyPrint, true);
    }

    void writeXML(XMLStreamWriter writer, boolean includeStorageIds, boolean prettyPrint, boolean includeName)
      throws XMLStreamException {
        this.writeEmptyItemElement(writer);
        this.writeAttributes(writer, includeStorageIds, includeName);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
    }

    @Override
    final void writeAttributes(XMLStreamWriter writer, boolean includeStorageIds, boolean includeName) throws XMLStreamException {
        super.writeAttributes(writer, includeStorageIds, includeName);
        this.writeSimpleAttributes(writer);
    }

    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        if (this.encodingId != null)
            this.writeAttr(writer, XMLConstants.ENCODING_ATTRIBUTE, this.encodingId.getId());
        if (!this.isAlwaysIndexed() && this.indexed)
            this.writeAttr(writer, XMLConstants.INDEXED_ATTRIBUTE, this.indexed);
    }

// Object

    @Override
    public String toString() {
        String string = super.toString();
        if (this.encodingId != null) {
            final String alias = EncodingIds.aliasForId(this.encodingId);
            final String id = this.encodingId.getId();
            string += alias.equals(id) ? " type \"" + id + "\"" : " type " + alias;
        }
        return string;
    }

    @Override
    String toStringName() {
        return String.format("\"%s\"", this.getFullName());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleSchemaField that = (SimpleSchemaField)obj;
        return Objects.equals(this.encodingId, that.encodingId) && this.indexed == that.indexed;
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
