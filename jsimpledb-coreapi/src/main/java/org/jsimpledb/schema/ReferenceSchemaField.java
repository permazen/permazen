
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.util.Diffs;

/**
 * A reference field in a {@link SchemaObjectType}.
 */
public class ReferenceSchemaField extends SimpleSchemaField {

    private DeleteAction onDelete;
    private boolean cascadeDelete;
    private boolean allowDeleted;
    private boolean allowDeletedSnapshot;
    private SortedSet<Integer> objectTypes;

    public ReferenceSchemaField() {
        this.setType(FieldType.REFERENCE_TYPE_NAME);
        this.setIndexed(true);
        this.setAllowDeletedSnapshot(true);
    }

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     *
     * @return desired behavior when a referenced object is deleted
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }
    public void setOnDelete(DeleteAction onDelete) {
        this.onDelete = onDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return whether deletion should cascade to the referred-to object
     */
    public boolean isCascadeDelete() {
        return this.cascadeDelete;
    }
    public void setCascadeDelete(boolean cascadeDelete) {
        this.cascadeDelete = cascadeDelete;
    }

    /**
     * Determine whether this field accepts references to deleted objects in normal (non-snapshot) transactions.
     *
     * @return whether deleted objects are allowed in normal transactions
     */
    public boolean isAllowDeleted() {
        return this.allowDeleted;
    }
    public void setAllowDeleted(boolean allowDeleted) {
        this.allowDeleted = allowDeleted;
    }

    /**
     * Determine whether this field accepts references to deleted objects in snapshot transactions.
     *
     * @return whether deleted objects are allowed in snapshot transactions
     */
    public boolean isAllowDeletedSnapshot() {
        return this.allowDeletedSnapshot;
    }
    public void setAllowDeletedSnapshot(boolean allowDeletedSnapshot) {
        this.allowDeletedSnapshot = allowDeletedSnapshot;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public SortedSet<Integer> getObjectTypes() {
        return this.objectTypes;
    }
    public void setObjectTypes(SortedSet<Integer> objectTypes) {
        this.objectTypes = objectTypes;
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (!FieldType.REFERENCE_TYPE_NAME.equals(this.getType())) {
            throw new InvalidSchemaException("invalid " + this + ": reference fields must have type `"
              + FieldType.REFERENCE_TYPE_NAME + "'");
        }
        if (!this.isIndexed())
            throw new IllegalArgumentException("invalid " + this + ": reference fields must always be indexed");
        if (this.getEncodingSignature() != 0)
            throw new IllegalArgumentException("invalid " + this + ": encoding signature must be zero");
        if (this.onDelete == null)
            throw new InvalidSchemaException("invalid " + this + ": no delete action specified");
        if (this.onDelete == DeleteAction.NOTHING && (!this.allowDeleted || !this.allowDeletedSnapshot)) {
            throw new InvalidSchemaException("invalid " + this + ": delete action " + this.onDelete
              + " is incompatible with disallowing assignment to deleted objects");
        }
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseReferenceSchemaField(this);
    }

// Compatibility

    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final ReferenceSchemaField that = (ReferenceSchemaField)that0;
        if (!super.isCompatibleWithInternal(that))
            return false;
        if (!this.onDelete.equals(that.onDelete))
            return false;
        if (this.cascadeDelete != that.cascadeDelete)
            return false;
        if (this.allowDeleted != that.allowDeleted)
            return false;
        if (this.allowDeletedSnapshot != that.allowDeletedSnapshot)
            return false;
        if (!(this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null))
            return false;
        return true;
    }

    @Override
    void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        super.writeCompatibilityHashData(output);
        output.writeBoolean(this.objectTypes != null);
        if (this.objectTypes != null) {
            output.writeInt(this.objectTypes.size());
            for (Integer storageId : this.objectTypes)
                output.writeInt(storageId);
        }
    }

    @Override
    boolean includeTypeInCompatibility() {
        return false;
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof ReferenceSchemaField)) {
            diffs.add("change type from " + other.getClass().getSimpleName() + " to " + this.getClass().getSimpleName());
            return diffs;
        }
        final ReferenceSchemaField that = (ReferenceSchemaField)other;
        if (!(this.onDelete != null ? this.onDelete.equals(that.onDelete) : that.onDelete == null))
            diffs.add("changed on-delete action from " + that.onDelete + " to " + this.onDelete);
        if (this.cascadeDelete != that.cascadeDelete)
            diffs.add("changed cascade delete from " + that.cascadeDelete + " to " + this.cascadeDelete);
        if (this.allowDeleted != that.allowDeleted)
            diffs.add("changed allowing assignement of deleted objects from " + that.allowDeleted + " to " + this.allowDeleted);
        if (this.allowDeletedSnapshot != that.allowDeletedSnapshot) {
            diffs.add("changed allowing assignement of deleted objects in snapshot transactions from "
              + that.allowDeletedSnapshot + " to " + this.allowDeletedSnapshot);
        }
        if (!(this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null))
            diffs.add("changed allowed object type storage IDs from " + that.objectTypes + " to " + this.objectTypes);
        return diffs;
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        super.readAttributes(reader, formatVersion);
        final String text = this.getAttr(reader, ON_DELETE_ATTRIBUTE, false);
        final DeleteAction action;
        if (text != null) {
            try {
                action = Enum.valueOf(DeleteAction.class, text);
            } catch (IllegalArgumentException e) {
                throw new XMLStreamException("invalid value `" + text
                  + " for \"" + ON_DELETE_ATTRIBUTE.getLocalPart() + "\" attribute in " + this, reader.getLocation());
            }
        } else
            action = DeleteAction.EXCEPTION;
        this.setOnDelete(action);
        final Boolean cascadeDeleteAttr = this.getBooleanAttr(reader, CASCADE_DELETE_ATTRIBUTE, false);
        if (cascadeDeleteAttr != null)
            this.setCascadeDelete(cascadeDeleteAttr);
        this.setAllowDeleted(action == DeleteAction.NOTHING);                   // defaults to false unless DeleteAction.NOTHING
        final Boolean allowDeletedAttr = this.getBooleanAttr(reader, ALLOW_DELETED_ATTRIBUTE, false);
        if (allowDeletedAttr != null)
            this.setAllowDeleted(allowDeletedAttr);
        this.setAllowDeletedSnapshot(true);                                     // defaults to true
        final Boolean allowDeletedSnapshotAttr = this.getBooleanAttr(reader, ALLOW_DELETED_SNAPSHOT_ATTRIBUTE, false);
        if (allowDeletedSnapshotAttr != null)
            this.setAllowDeletedSnapshot(allowDeletedSnapshotAttr);
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {

        // Any restrictions?
        if (!this.expect(reader, true, OBJECT_TYPES_TAG)) {
            this.objectTypes = null;
            return;
        }

        // Read list of zero or more permitted storage ID
        this.objectTypes = new TreeSet<>();
        while (this.expect(reader, true, OBJECT_TYPE_TAG)) {
            this.objectTypes.add(this.getIntAttr(reader, STORAGE_ID_ATTRIBUTE));
            this.expectClose(reader);           // </ObjectType>
        }

        // Read closing </ReferenceField>
        this.expectClose(reader);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        if (this.objectTypes != null)
            writer.writeStartElement(REFERENCE_FIELD_TAG.getNamespaceURI(), REFERENCE_FIELD_TAG.getLocalPart());
        else
            writer.writeEmptyElement(REFERENCE_FIELD_TAG.getNamespaceURI(), REFERENCE_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
        if (this.onDelete != null)
            writer.writeAttribute(ON_DELETE_ATTRIBUTE.getNamespaceURI(), ON_DELETE_ATTRIBUTE.getLocalPart(), this.onDelete.name());
        if (this.allowDeleted != (this.onDelete == DeleteAction.NOTHING)) {
            writer.writeAttribute(ALLOW_DELETED_ATTRIBUTE.getNamespaceURI(), ALLOW_DELETED_ATTRIBUTE.getLocalPart(),
              "" + this.allowDeleted);
        }
        if (!this.allowDeletedSnapshot) {
            writer.writeAttribute(ALLOW_DELETED_SNAPSHOT_ATTRIBUTE.getNamespaceURI(),
              ALLOW_DELETED_SNAPSHOT_ATTRIBUTE.getLocalPart(), "" + this.allowDeletedSnapshot);
        }
        if (this.objectTypes != null) {
            writer.writeStartElement(OBJECT_TYPES_TAG.getNamespaceURI(), OBJECT_TYPES_TAG.getLocalPart());
            for (int storageId : this.objectTypes) {
                writer.writeEmptyElement(OBJECT_TYPE_TAG.getNamespaceURI(), OBJECT_TYPE_TAG.getLocalPart());
                writer.writeAttribute(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + storageId);
            }
            writer.writeEndElement();           // </ObjectTypes>
            writer.writeEndElement();           // </ReferenceField>
        }
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        // don't need to write type or indexed
        if (this.cascadeDelete) {
            writer.writeAttribute(CASCADE_DELETE_ATTRIBUTE.getNamespaceURI(), CASCADE_DELETE_ATTRIBUTE.getLocalPart(),
              "" + this.cascadeDelete);
        }
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceSchemaField that = (ReferenceSchemaField)obj;
        return this.onDelete == that.onDelete
          && this.cascadeDelete == that.cascadeDelete
          && this.allowDeleted == that.allowDeleted
          && this.allowDeletedSnapshot == that.allowDeletedSnapshot
          && (this.objectTypes != null ? this.objectTypes.equals(that.objectTypes) : that.objectTypes == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.cascadeDelete ? 1 : 0)
          ^ (this.allowDeleted ? 2 : 0)
          ^ (this.allowDeletedSnapshot ? 4 : 0)
          ^ (this.onDelete != null ? this.onDelete.hashCode() : 0)
          ^ (this.objectTypes != null ? this.objectTypes.hashCode() : 0);
    }

// Cloneable

    @Override
    public ReferenceSchemaField clone() {
        final ReferenceSchemaField clone = (ReferenceSchemaField)super.clone();
        if (clone.objectTypes != null)
            clone.objectTypes = new TreeSet<>(clone.objectTypes);
        return clone;
    }
}

