
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;

/**
 * One object type in a {@link SchemaModel}.
 */
public class SchemaObjectType extends AbstractSchemaItem {

    private /*final*/ TreeMap<Integer, SchemaField> schemaFields = new TreeMap<>();
    private /*final*/ TreeMap<Integer, SchemaCompositeIndex> schemaCompositeIndexes = new TreeMap<>();

    /**
     * Get this object type's {@link SchemaField}s, indexed by storage ID.
     */
    public SortedMap<Integer, SchemaField> getSchemaFields() {
        return this.schemaFields;
    }

    /**
     * Get the {@link SchemaCompositeIndex}s defined on this object type's fields, indexed by storage ID.
     */
    public SortedMap<Integer, SchemaCompositeIndex> getSchemaCompositeIndexes() {
        return this.schemaCompositeIndexes;
    }

    @Override
    void validate() {
        super.validate();

        // Validate fields and verify field names are unique
        final TreeMap<String, SchemaField> fieldsByName = new TreeMap<>();
        for (SchemaField field : this.schemaFields.values()) {
            field.validate();
            final String fieldName = field.getName();
            if (fieldsByName.put(fieldName, field) != null)
                throw new InvalidSchemaException("duplicate field name `" + fieldName + "'");
        }

        // Validate composite indexes and verify index names are unique
        final TreeMap<String, SchemaCompositeIndex> compositeIndexesByName = new TreeMap<>();
        for (SchemaCompositeIndex index : this.schemaCompositeIndexes.values()) {
            index.validate();
            final String indexName = index.getName();
            if (compositeIndexesByName.put(indexName, index) != null)
                throw new InvalidSchemaException("duplicate composite index name `" + indexName + "'");
        }

        // Verify indexes index valid simple fields that are not sub-fields of complex fields
        for (SchemaCompositeIndex index : this.schemaCompositeIndexes.values()) {
            for (int storageId : index.getIndexedFields()) {
                final SchemaField field = this.schemaFields.get(storageId);
                if (!(field instanceof SimpleSchemaField))
                    throw new InvalidSchemaException(index + " indexes unknown or invalid field with storage ID " + storageId);
            }
        }

        // Verify there are no duplicate composite indexes
        final HashMap<List<Integer>, SchemaCompositeIndex> compositeIndexFields = new HashMap<>();
        for (SchemaCompositeIndex index : this.schemaCompositeIndexes.values()) {
            final SchemaCompositeIndex previous = compositeIndexFields.put(index.getIndexedFields(), index);
            if (previous != null)
                throw new InvalidSchemaException("duplicate " + index + " (duplicates " + previous + ")");
        }
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.schemaFields.clear();
        this.schemaCompositeIndexes.clear();
        boolean seenIndex = false;
        for (AbstractSchemaItem item;
          (item = this.readMappedType(reader, true, SchemaModel.FIELD_OR_COMPOSITE_INDEX_TAG_MAP)) != null; ) {
            if (item instanceof SchemaField) {
                if (seenIndex)
                    throw new XMLStreamException("indexes must appear after fields", reader.getLocation());
                final SchemaField field = (SchemaField)item;
                field.readXML(reader, formatVersion);
                final int storageId = field.getStorageId();
                final SchemaField previous = this.schemaFields.put(storageId, field);
                if (previous != null) {
                    throw new XMLStreamException("duplicate use of storage ID " + storageId
                      + " for both " + previous + " and " + field + " in " + this, reader.getLocation());
                }
            } else if (item instanceof SchemaCompositeIndex) {
                final SchemaCompositeIndex index = (SchemaCompositeIndex)item;
                index.readXML(reader, formatVersion);
                final int storageId = index.getStorageId();
                final SchemaCompositeIndex previous = this.schemaCompositeIndexes.put(storageId, index);
                if (previous != null) {
                    throw new XMLStreamException("duplicate use of storage ID " + storageId
                      + " for both " + previous + " and " + index + " in " + this, reader.getLocation());
                }
                seenIndex = true;
            } else
                throw new RuntimeException("internal error");
        }
    }

    @Override
    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        if (this.schemaFields.isEmpty() && this.schemaCompositeIndexes.isEmpty()) {
            writer.writeEmptyElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
            this.writeAttributes(writer);
            return;
        }
        writer.writeStartElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
        this.writeAttributes(writer);
        for (SchemaField schemaField : this.schemaFields.values())
            schemaField.writeXML(writer);
        for (SchemaCompositeIndex schemaCompositeIndex : this.schemaCompositeIndexes.values())
            schemaCompositeIndex.writeXML(writer);
        writer.writeEndElement();
    }

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final SchemaObjectType that = (SchemaObjectType)that0;
        if (!this.schemaFields.keySet().equals(that.schemaFields.keySet()))
            return false;
        for (int storageId : this.schemaFields.keySet()) {
            final SchemaField thisSchemaField = this.schemaFields.get(storageId);
            final SchemaField thatSchemaField = that.schemaFields.get(storageId);
            if (!thisSchemaField.isCompatibleWith(thatSchemaField))
                return false;
        }
        if (!this.schemaCompositeIndexes.keySet().equals(that.schemaCompositeIndexes.keySet()))
            return false;
        for (int storageId : this.schemaCompositeIndexes.keySet()) {
            final SchemaCompositeIndex thisSchemaIndex = this.schemaCompositeIndexes.get(storageId);
            final SchemaCompositeIndex thatSchemaIndex = that.schemaCompositeIndexes.get(storageId);
            if (!thisSchemaIndex.isCompatibleWith(thatSchemaIndex))
                return false;
        }
        return true;
    }

// Object

    @Override
    public String toString() {
        return "object " + super.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SchemaObjectType that = (SchemaObjectType)obj;
        return this.schemaFields.equals(that.schemaFields) && this.schemaCompositeIndexes.equals(that.schemaCompositeIndexes);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.schemaFields.hashCode() ^ this.schemaCompositeIndexes.hashCode();
    }

// Cloneable

    @Override
    @SuppressWarnings("unchecked")
    public SchemaObjectType clone() {
        final SchemaObjectType clone = (SchemaObjectType)super.clone();
        clone.schemaFields = (TreeMap<Integer, SchemaField>)clone.schemaFields.clone();
        for (Map.Entry<Integer, SchemaField> entry : clone.schemaFields.entrySet())
            entry.setValue(entry.getValue().clone());
        clone.schemaCompositeIndexes = (TreeMap<Integer, SchemaCompositeIndex>)clone.schemaCompositeIndexes.clone();
        for (Map.Entry<Integer, SchemaCompositeIndex> entry : clone.schemaCompositeIndexes.entrySet())
            entry.setValue(entry.getValue().clone());
        return clone;
    }
}

