
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * An index on two or more fields in a {@link SchemaObjectType}.
 */
public class SchemaCompositeIndex extends AbstractObjectTypeMember implements DiffGenerating<SchemaCompositeIndex> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.COMPOSITE_INDEX;

    private List<String> indexedFields = new ArrayList<>();

// Properties

    /**
     * Get the fields that comprise this index.
     *
     * @return names of indexed fields
     */
    public List<String> getIndexedFields() {
        return this.indexedFields;
    }

// Lockdown

    @Override
    void lockDown1() {
        super.lockDown1();
        this.indexedFields = Collections.unmodifiableList(this.indexedFields);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.indexedFields.size() < 2 || this.indexedFields.size() > Database.MAX_INDEXED_FIELDS) {
            throw new InvalidSchemaException(String.format(
              "invalid %s: between 2 and %d fields must be specified", this, Database.MAX_INDEXED_FIELDS));
        }
        final HashSet<String> namesSeen = new HashSet<>();
        for (String fieldName : this.indexedFields) {
            if (fieldName == null)
                throw new InvalidSchemaException(String.format("invalid %s: field list contains null", this));
            if (!namesSeen.add(fieldName))
                throw new InvalidSchemaException(String.format("invalid %s: duplicate field \"%s\"", this, fieldName));
            final SchemaField field = this.getObjectType().getSchemaFields().get(fieldName);
            if (field == null)
                throw new InvalidSchemaException(String.format("%s indexes unknown field \"%s\"", this, fieldName));
            if (!(field instanceof SimpleSchemaField))
                throw new InvalidSchemaException(String.format("%s indexes invalid field \"%s\"", this, fieldName));
        }
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.indexedFields.clear();
        while (this.expect(reader, true, XMLConstants.FIELD_TAG)) {
            this.indexedFields.add(this.getAttr(reader, XMLConstants.NAME_ATTRIBUTE));
            this.expectClose(reader);   // </IndexedField>
        }
        if (this.indexedFields instanceof ArrayList)
            ((ArrayList<?>)this.indexedFields).trimToSize();
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean prettyPrint) throws XMLStreamException {
        this.writeStartElement(writer, XMLConstants.COMPOSITE_INDEX_TAG);
        this.writeAttributes(writer, true);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
        for (String fieldName : this.indexedFields) {
            this.writeEmptyElement(writer, XMLConstants.FIELD_TAG);
            this.writeAttr(writer, XMLConstants.NAME_ATTRIBUTE, fieldName);
        }
        writer.writeEndElement();           // </CompositeIndex>
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        output.writeInt(this.indexedFields.size());
        final SchemaObjectType objectType = this.getObjectType();
        output.writeBoolean(objectType != null);
        if (objectType != null) {
            for (String fieldName : this.indexedFields) {
                final SchemaField field = objectType.getSchemaFields().get(fieldName);
                output.writeBoolean(field != null);
                if (field != null)
                    field.writeSchemaIdHashData(output, false);
            }
        }
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaCompositeIndex that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        if (!this.indexedFields.equals(that.indexedFields))
            diffs.add(String.format("changed %s from %s to %s", "indexed fields", that.indexedFields, this.indexedFields));
        return diffs;
    }

// Object

    @Override
    public String toString() {
        return "composite index " + this.toStringName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SchemaCompositeIndex that = (SchemaCompositeIndex)obj;
        return this.indexedFields.equals(that.indexedFields);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.indexedFields.hashCode();
    }

// Cloneable

    @Override
    @SuppressWarnings("unchecked")
    public SchemaCompositeIndex clone() {
        final SchemaCompositeIndex clone = (SchemaCompositeIndex)super.clone();
        clone.indexedFields = new ArrayList<>(clone.indexedFields);
        return clone;
    }
}
