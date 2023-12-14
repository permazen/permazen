
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.InvalidSchemaException;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;
import io.permazen.util.NavigableSets;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * One object type in a {@link SchemaModel}.
 */
public class SchemaObjectType extends SchemaItem implements DiffGenerating<SchemaObjectType> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.OBJECT_TYPE;

    private NavigableMap<String, SchemaField> fields = new TreeMap<>();
    private NavigableMap<String, SchemaCompositeIndex> indexes = new TreeMap<>();

// Properties

    /**
     * Get this object type's {@link SchemaField}s, indexed by name.
     *
     * @return fields in this object type
     */
    public NavigableMap<String, SchemaField> getSchemaFields() {
        return this.fields;
    }

    /**
     * Get the {@link SchemaCompositeIndex}s defined on this object type's fields, indexed by name.
     *
     * @return composite indexes in this object type
     */
    public NavigableMap<String, SchemaCompositeIndex> getSchemaCompositeIndexes() {
        return this.indexes;
    }

// Recursion

    @Override
    public void visitSchemaItems(Consumer<? super SchemaItem> visitor) {
        super.visitSchemaItems(visitor);
        this.fields.values().forEach(field -> field.visitSchemaItems(visitor));
        this.indexes.values().forEach(index -> index.visitSchemaItems(visitor));
    }

// Lockdown

    @Override
    public void lockDown() {
        super.lockDown();

        // Set simple sub-fields' parent field to null
        this.fields.values().stream()
          .filter(SimpleSchemaField.class::isInstance)
          .map(SimpleSchemaField.class::cast)
          .iterator()
          .forEachRemaining(field -> field.setParent(null));

        // Set fields' and indexs' parent object types to this instance
        Stream.concat(this.fields.values().stream(), this.indexes.values().stream())
          .iterator()
          .forEachRemaining(item -> item.setObjectType(this));

        // Lock down maps
        this.fields = this.lockDownMap(this.fields);
        this.indexes = this.lockDownMap(this.indexes);
    }

// Validation

    @Override
    void validate() {
        super.validate();

        // Verify mapped field and index names
        this.verifyMappedNames("field", this.fields);
        this.verifyMappedNames("composite index", this.indexes);

        // Validate fields and composite indexes
        this.fields.values().forEach(SchemaField::validate);
        this.indexes.values().forEach(SchemaCompositeIndex::validate);

        // Verify object type back references
        this.verifyBackReferences("object type", this.fields, SchemaField::getObjectType);
        this.verifyBackReferences("object type", this.indexes, SchemaCompositeIndex::getObjectType);

        // Verify top level simple fields have no complex parent
        this.fields.values().forEach(field -> {
            if (field instanceof SimpleSchemaField) {
                final ComplexSchemaField parent = ((SimpleSchemaField)field).getParent();
                if (parent != null)
                    throw new InvalidSchemaException(String.format("top level %s has non-null parent %s", field, parent));
            }
        });

        // Verify there are no duplicate composite indexes and that field and composite index names don't conflict
        final HashMap<List<String>, SchemaCompositeIndex> fieldListMap = new HashMap<>();
        for (SchemaCompositeIndex index : this.indexes.values()) {
            final SchemaCompositeIndex previous = fieldListMap.put(index.getIndexedFields(), index);
            if (previous != null) {
                throw new InvalidSchemaException(String.format(
                  "composite index \"%s\" duplicates composite index \"%s\"", index.getName(), previous.getName()));
            }
            if (this.fields.containsKey(index.getName())) {
                throw new InvalidSchemaException(String.format(
                  "composite index name \"%s\" is already used as a field name", index.getName()));
            }
        }
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.fields.clear();
        this.indexes.clear();
        boolean seenCompositeIndex = false;
        for (SchemaItem item; (item = this.readMappedType(reader, true, SchemaModel.FIELD_OR_COMPOSITE_INDEX_TAG_MAP)) != null; ) {
            if (item instanceof SchemaField) {
                if (seenCompositeIndex)
                    throw new XMLStreamException("composite indexes must appear after fields", reader.getLocation());
                final SchemaField field = (SchemaField)item;
                field.readXML(reader, formatVersion);
                final String fieldName = field.getName();
                if (fieldName == null)
                    throw this.newInvalidAttributeException(reader, XMLConstants.NAME_ATTRIBUTE, "name is required");
                final SchemaField previous = this.fields.put(fieldName, field);
                if (previous != null) {
                    throw new XMLStreamException(String.format(
                      "duplicate %s name \"%s\" in %s", "field", fieldName, this), reader.getLocation());
                }
                field.setObjectType(this);
            } else if (item instanceof SchemaCompositeIndex) {
                final SchemaCompositeIndex index = (SchemaCompositeIndex)item;
                index.readXML(reader, formatVersion);
                final String indexName = index.getName();
                if (indexName == null)
                    throw this.newInvalidAttributeException(reader, XMLConstants.NAME_ATTRIBUTE, "name is required");
                final SchemaCompositeIndex previous = this.indexes.put(indexName, index);
                if (previous != null) {
                    throw new XMLStreamException(String.format(
                      "duplicate %s name \"%s\" in %s", "composite index", indexName, this), reader.getLocation());
                }
                index.setObjectType(this);
                seenCompositeIndex = true;
            } else
                throw new RuntimeException("internal error");
        }
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean prettyPrint) throws XMLStreamException {

        // Totally empty?
        if (this.fields.isEmpty() && this.indexes.isEmpty()) {
            this.writeEmptyElement(writer, XMLConstants.OBJECT_TYPE_TAG);
            this.writeAttributes(writer);
            if (prettyPrint)
                this.writeSchemaIdComment(writer);
            return;
        }

        // Write fields and/or composite indexes
        this.writeStartElement(writer, XMLConstants.OBJECT_TYPE_TAG);
        this.writeAttributes(writer);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
        for (SchemaField schemaField : this.fields.values())
            schemaField.writeXML(writer, prettyPrint);
        for (SchemaCompositeIndex schemaIndex : this.indexes.values())
            schemaIndex.writeXML(writer, prettyPrint);
        writer.writeEndElement();
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

    // When computing our own schema ID, we don't include fields or indexes, meaning the schema ID of a SchemaObjectType
    // depends only on the type name. When compuing a schema ID as part of a SchemaModel, we include fields and indexes.
    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        output.writeBoolean(forSchemaModel);
        if (forSchemaModel) {
            output.writeInt(this.fields.size());
            for (SchemaField field : this.fields.values())
                field.writeSchemaIdHashData(output, true);
            output.writeInt(this.indexes.size());
            for (SchemaCompositeIndex index : this.indexes.values())
                index.writeSchemaIdHashData(output, true);
        }
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaObjectType that) {
        final Diffs diffs = new Diffs(super.differencesFrom(that));

        // Check fields
        final NavigableSet<String> allFieldNames = NavigableSets.union(
          this.fields.navigableKeySet(), that.fields.navigableKeySet());
        for (String fieldName : allFieldNames) {
            final SchemaField thisField = this.fields.get(fieldName);
            final SchemaField thatField = that.fields.get(fieldName);
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
                    public Diffs caseEnumArraySchemaField(EnumArraySchemaField field) {
                        return field.differencesFrom((EnumArraySchemaField)thatField);
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
        final NavigableSet<String> allIndexNames = NavigableSets.union(
          this.indexes.navigableKeySet(), that.indexes.navigableKeySet());
        for (String indexName : allIndexNames) {
            final SchemaCompositeIndex thisIndex = this.indexes.get(indexName);
            final SchemaCompositeIndex thatIndex = that.indexes.get(indexName);
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
        return "object type " + this.toStringName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SchemaObjectType that = (SchemaObjectType)obj;
        return this.fields.equals(that.fields)
          && this.indexes.equals(that.indexes);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.fields.hashCode() ^ this.indexes.hashCode();
    }

// Cloneable

    @Override
    @SuppressWarnings("unchecked")
    public SchemaObjectType clone() {
        final SchemaObjectType clone = (SchemaObjectType)super.clone();
        clone.fields = this.cloneMap(clone.fields);
        clone.indexes = this.cloneMap(clone.indexes);
        Stream.concat(clone.fields.values().stream(), clone.indexes.values().stream())
          .iterator()
          .forEachRemaining(item -> item.setObjectType(clone));
        return clone;
    }
}
