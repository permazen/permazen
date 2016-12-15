
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.util.DiffGenerating;
import org.jsimpledb.util.Diffs;
import org.jsimpledb.util.NavigableSets;

/**
 * One object type in a {@link SchemaModel}.
 */
public class SchemaObjectType extends AbstractSchemaItem implements DiffGenerating<SchemaObjectType> {

    private /*final*/ TreeMap<Integer, SchemaField> schemaFields = new TreeMap<>();
    private /*final*/ TreeMap<Integer, SchemaCompositeIndex> schemaCompositeIndexes = new TreeMap<>();

    /**
     * Get this object type's {@link SchemaField}s, indexed by storage ID.
     *
     * @return fields in this object type
     */
    public SortedMap<Integer, SchemaField> getSchemaFields() {
        return this.schemaFields;
    }

    /**
     * Get the {@link SchemaCompositeIndex}s defined on this object type's fields, indexed by storage ID.
     *
     * @return composite indexes in this object type
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
        final ArrayList<SchemaField> fieldList = new ArrayList<>(this.schemaFields.values());
        Collections.sort(fieldList, Comparator.comparing(SchemaField::getName));
        for (SchemaField schemaField : fieldList)
            schemaField.writeXML(writer);
        final ArrayList<SchemaCompositeIndex> indexList = new ArrayList<>(this.schemaCompositeIndexes.values());
        Collections.sort(indexList, Comparator.comparing(SchemaCompositeIndex::getName));
        for (SchemaCompositeIndex schemaCompositeIndex : indexList)
            schemaCompositeIndex.writeXML(writer);
        writer.writeEndElement();
    }

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final SchemaObjectType that = (SchemaObjectType)that0;
        if (!AbstractSchemaItem.allAreCompatible(this.schemaFields, that.schemaFields))
            return false;
        if (!AbstractSchemaItem.allAreCompatible(this.schemaCompositeIndexes, that.schemaCompositeIndexes))
            return false;
        return true;
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaObjectType that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));

        // Check fields
        final NavigableSet<Integer> allFieldIds = NavigableSets.union(
          this.schemaFields.navigableKeySet(), that.schemaFields.navigableKeySet());
        for (int storageId : allFieldIds) {
            final SchemaField thisField = this.schemaFields.get(storageId);
            final SchemaField thatField = that.schemaFields.get(storageId);
            if (thatField != null && (thisField == null || !thisField.getClass().equals(thatField.getClass())))
                diffs.add("removed " + thatField);
            else if (thisField != null && (thatField == null || !thatField.getClass().equals(thisField.getClass())))
                diffs.add("added " + thisField);
            else {
                final Diffs fieldDiffs = thisField.visit(new SchemaFieldSwitch<Diffs>() {
                    @Override
                    public Diffs caseSetSchemaField(SetSchemaField field) {
                        return field.differencesFrom((SetSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseListSchemaField(ListSchemaField field) {
                        return field.differencesFrom((ListSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseMapSchemaField(MapSchemaField field) {
                        return field.differencesFrom((MapSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseSimpleSchemaField(SimpleSchemaField field) {
                        return field.differencesFrom((SimpleSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseReferenceSchemaField(ReferenceSchemaField field) {
                        return field.differencesFrom((ReferenceSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseEnumSchemaField(EnumSchemaField field) {
                        return field.differencesFrom((EnumSchemaField)thatField);
                    }
                    @Override
                    public Diffs caseCounterSchemaField(CounterSchemaField field) {
                        return new Diffs();
                    }
                });
                if (!fieldDiffs.isEmpty())
                    diffs.add("changed " + thatField, fieldDiffs);
            }
        }

        // Check composite indexes
        final NavigableSet<Integer> allIndexIds = NavigableSets.union(
          this.schemaCompositeIndexes.navigableKeySet(), that.schemaCompositeIndexes.navigableKeySet());
        for (int storageId : allIndexIds) {
            final SchemaCompositeIndex thisIndex = this.schemaCompositeIndexes.get(storageId);
            final SchemaCompositeIndex thatIndex = that.schemaCompositeIndexes.get(storageId);
            if (thisIndex == null)
                diffs.add("removed " + thatIndex);
            else if (thatIndex == null)
                diffs.add("added " + thisIndex);
            else {
                final Diffs indexDiffs = thisIndex.differencesFrom(thatIndex);
                if (!indexDiffs.isEmpty())
                    diffs.add("changed " + thatIndex, indexDiffs);
            }
        }

        // Done
        return diffs;
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

