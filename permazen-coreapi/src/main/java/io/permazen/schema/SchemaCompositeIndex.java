
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

/**
 * A composite index associated with a {@link SchemaObjectType}.
 */
public class SchemaCompositeIndex extends AbstractSchemaItem implements DiffGenerating<SchemaCompositeIndex> {

    private /*final*/ List<Integer> indexedFields = new ArrayList<>();

    /**
     * Get the fields that comprise this index.
     *
     * @return storage IDs of indexed fields
     */
    public List<Integer> getIndexedFields() {
        return this.indexedFields;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        this.indexedFields = Collections.unmodifiableList(this.indexedFields);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.indexedFields.size() < 2 || this.indexedFields.size() > Database.MAX_INDEXED_FIELDS) {
            throw new InvalidSchemaException("invalid " + this + ": between 2 and "
              + Database.MAX_INDEXED_FIELDS + " fields must be specified");
        }
        final HashSet<Integer> idsSeen = new HashSet<>();
        for (Integer indexedField : this.indexedFields) {
            final int storageId = indexedField;
            if (!idsSeen.add(storageId))
                throw new InvalidSchemaException("invalid " + this + ": duplicate field in composite index: " + storageId);
        }
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.indexedFields.clear();
        while (this.expect(reader, true, XMLConstants.INDEXED_FIELD_TAG)) {
            this.indexedFields.add(this.getIntAttr(reader, XMLConstants.STORAGE_ID_ATTRIBUTE));
            this.expectClose(reader);   // </IndexedField>
        }
        if (this.indexedFields instanceof ArrayList)
            ((ArrayList<?>)this.indexedFields).trimToSize();
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement(XMLConstants.COMPOSITE_INDEX_TAG.getNamespaceURI(),
          XMLConstants.COMPOSITE_INDEX_TAG.getLocalPart());
        this.writeAttributes(writer, true);
        for (int storageId : this.indexedFields) {
            writer.writeEmptyElement(XMLConstants.INDEXED_FIELD_TAG.getNamespaceURI(),
              XMLConstants.INDEXED_FIELD_TAG.getLocalPart());
            writer.writeAttribute(XMLConstants.STORAGE_ID_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + storageId);
        }
        writer.writeEndElement();           // </CompositeIndex>
    }

// Compatibility

    boolean isCompatibleWith(SchemaCompositeIndex that) {
        return this.indexedFields.equals(that.indexedFields);
    }

    @Override
    void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        super.writeCompatibilityHashData(output);
        output.writeInt(this.indexedFields.size());
        for (Integer storageId : this.indexedFields)
            output.writeInt(storageId);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaCompositeIndex that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));
        if (!this.indexedFields.equals(that.indexedFields))
            diffs.add("changed indexed field storage IDs from " + that.indexedFields + " to " + this.indexedFields);
        return diffs;
    }

// Object

    @Override
    public String toString() {
        return "composite index " + super.toString();
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

