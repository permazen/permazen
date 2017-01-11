
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.util.DiffGenerating;
import org.jsimpledb.util.Diffs;

/**
 * A composite index associated with a {@link SchemaObjectType}.
 */
public class SchemaCompositeIndex extends AbstractSchemaItem implements DiffGenerating<SchemaCompositeIndex> {

    private /*final*/ ArrayList<Integer> indexedFields = new ArrayList<>();

    /**
     * Get the fields that comprise this index.
     *
     * @return storage IDs of indexed fields
     */
    public List<Integer> getIndexedFields() {
        return this.indexedFields;
    }

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
        while (this.expect(reader, true, INDEXED_FIELD_TAG)) {
            this.indexedFields.add(this.getIntAttr(reader, STORAGE_ID_ATTRIBUTE));
            this.expectClose(reader);   // </IndexedField>
        }
        this.indexedFields.trimToSize();
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement(COMPOSITE_INDEX_TAG.getNamespaceURI(), COMPOSITE_INDEX_TAG.getLocalPart());
        this.writeAttributes(writer, true);
        for (int storageId : this.indexedFields) {
            writer.writeEmptyElement(INDEXED_FIELD_TAG.getNamespaceURI(), INDEXED_FIELD_TAG.getLocalPart());
            writer.writeAttribute(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + storageId);
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
        clone.indexedFields = (ArrayList<Integer>)clone.indexedFields.clone();
        return clone;
    }
}

