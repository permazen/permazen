
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.DeleteAction;
import io.permazen.core.InvalidSchemaException;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * A reference field in a {@link SchemaObjectType}.
 */
public class ReferenceSchemaField extends SimpleSchemaField {

    private DeleteAction inverseDelete;
    private boolean forwardDelete;
    private boolean allowDeleted;
    private boolean allowDeletedSnapshot;
    private NavigableSet<Integer> objectTypes;

    public ReferenceSchemaField() {
        this.setAllowDeletedSnapshot(true);
    }

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     *
     * @return desired behavior when a referenced object is deleted
     */
    public DeleteAction getInverseDelete() {
        return this.inverseDelete;
    }
    public void setInverseDelete(DeleteAction inverseDelete) {
        this.verifyNotLockedDown();
        this.inverseDelete = inverseDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return whether deletion should cascade to the referred-to object
     */
    public boolean isForwardDelete() {
        return this.forwardDelete;
    }
    public void setForwardDelete(boolean forwardDelete) {
        this.verifyNotLockedDown();
        this.forwardDelete = forwardDelete;
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
        this.verifyNotLockedDown();
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
        this.verifyNotLockedDown();
        this.allowDeletedSnapshot = allowDeletedSnapshot;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * @return storage IDs of allowed object types, or null if there is no restriction
     */
    public NavigableSet<Integer> getObjectTypes() {
        return this.objectTypes;
    }
    public void setObjectTypes(NavigableSet<Integer> objectTypes) {
        this.verifyNotLockedDown();
        this.objectTypes = objectTypes;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        if (this.objectTypes != null)
            this.objectTypes = Collections.unmodifiableNavigableSet(this.objectTypes);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.inverseDelete == null)
            throw new InvalidSchemaException("invalid " + this + ": no delete action specified");
        if (this.inverseDelete == DeleteAction.IGNORE && (!this.allowDeleted || !this.allowDeletedSnapshot)) {
            throw new InvalidSchemaException("invalid " + this + ": delete action " + this.inverseDelete
              + " is incompatible with disallowing assignment to deleted objects");
        }
    }

    @Override
    boolean hasFixedEncoding() {
        return true;
    }

    @Override
    boolean isAlwaysIndexed() {
        return true;
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseReferenceSchemaField(this);
    }

// Compatibility

    // Reference fields are differentiated only by what types they can refer to
    @Override
    boolean isCompatibleType(SimpleSchemaField field) {
        final ReferenceSchemaField that = (ReferenceSchemaField)field;
        return Objects.equals(this.objectTypes, that.objectTypes);
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

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof ReferenceSchemaField)) {
            diffs.add("change type from " + other.getClass().getSimpleName() + " to " + this.getClass().getSimpleName());
            return diffs;
        }
        final ReferenceSchemaField that = (ReferenceSchemaField)other;
        if (!Objects.equals(this.inverseDelete, that.inverseDelete))
            diffs.add("changed on-delete action from " + that.inverseDelete + " to " + this.inverseDelete);
        if (this.forwardDelete != that.forwardDelete)
            diffs.add("changed cascade delete from " + that.forwardDelete + " to " + this.forwardDelete);
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
        this.setInverseDelete(this.readAttr(reader, DeleteAction.class,
          XMLConstants.INVERSE_DELETE_ATTRIBUTE, DeleteAction.EXCEPTION));
        final Boolean forwardDeleteAttr = this.getBooleanAttr(reader, XMLConstants.FORWARD_DELETE_ATTRIBUTE, false);
        if (forwardDeleteAttr != null)
            this.setForwardDelete(forwardDeleteAttr);
        this.setAllowDeleted(this.getInverseDelete() == DeleteAction.IGNORE);   // defaults to false unless DeleteAction.IGNORE
        final Boolean allowDeletedAttr = this.getBooleanAttr(reader, XMLConstants.ALLOW_DELETED_ATTRIBUTE, false);
        if (allowDeletedAttr != null)
            this.setAllowDeleted(allowDeletedAttr);
        this.setAllowDeletedSnapshot(true);                                     // defaults to true
        final Boolean allowDeletedSnapshotAttr = this.getBooleanAttr(reader, XMLConstants.ALLOW_DELETED_SNAPSHOT_ATTRIBUTE, false);
        if (allowDeletedSnapshotAttr != null)
            this.setAllowDeletedSnapshot(allowDeletedSnapshotAttr);
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {

        // Any restrictions?
        if (!this.expect(reader, true, XMLConstants.OBJECT_TYPES_TAG)) {
            this.objectTypes = null;
            return;
        }

        // Read list of zero or more permitted storage ID
        this.objectTypes = new TreeSet<>();
        while (this.expect(reader, true, XMLConstants.OBJECT_TYPE_TAG)) {
            this.objectTypes.add(this.getIntAttr(reader, XMLConstants.STORAGE_ID_ATTRIBUTE));
            this.expectClose(reader);           // </ObjectType>
        }

        // Read closing </ReferenceField>
        this.expectClose(reader);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        if (this.objectTypes != null) {
            writer.writeStartElement(XMLConstants.REFERENCE_FIELD_TAG.getNamespaceURI(),
              XMLConstants.REFERENCE_FIELD_TAG.getLocalPart());
        } else {
            writer.writeEmptyElement(XMLConstants.REFERENCE_FIELD_TAG.getNamespaceURI(),
              XMLConstants.REFERENCE_FIELD_TAG.getLocalPart());
        }
        this.writeAttributes(writer, includeName);
        if (this.inverseDelete != null) {
            writer.writeAttribute(XMLConstants.INVERSE_DELETE_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.INVERSE_DELETE_ATTRIBUTE.getLocalPart(), this.inverseDelete.name());
        }
        if (this.allowDeleted != (this.inverseDelete == DeleteAction.IGNORE)) {
            writer.writeAttribute(XMLConstants.ALLOW_DELETED_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.ALLOW_DELETED_ATTRIBUTE.getLocalPart(), "" + this.allowDeleted);
        }
        if (!this.allowDeletedSnapshot) {
            writer.writeAttribute(XMLConstants.ALLOW_DELETED_SNAPSHOT_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.ALLOW_DELETED_SNAPSHOT_ATTRIBUTE.getLocalPart(), "" + this.allowDeletedSnapshot);
        }
        if (this.objectTypes != null) {
            writer.writeStartElement(XMLConstants.OBJECT_TYPES_TAG.getNamespaceURI(), XMLConstants.OBJECT_TYPES_TAG.getLocalPart());
            for (int storageId : this.objectTypes) {
                writer.writeEmptyElement(XMLConstants.OBJECT_TYPE_TAG.getNamespaceURI(),
                  XMLConstants.OBJECT_TYPE_TAG.getLocalPart());
                writer.writeAttribute(XMLConstants.STORAGE_ID_ATTRIBUTE.getNamespaceURI(),
                  XMLConstants.STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + storageId);
            }
            writer.writeEndElement();           // </ObjectTypes>
            writer.writeEndElement();           // </ReferenceField>
        }
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        super.writeSimpleAttributes(writer);
        if (this.forwardDelete) {
            writer.writeAttribute(XMLConstants.FORWARD_DELETE_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.FORWARD_DELETE_ATTRIBUTE.getLocalPart(), "" + this.forwardDelete);
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
        return this.inverseDelete == that.inverseDelete
          && this.forwardDelete == that.forwardDelete
          && this.allowDeleted == that.allowDeleted
          && this.allowDeletedSnapshot == that.allowDeletedSnapshot
          && Objects.equals(this.objectTypes, that.objectTypes);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.forwardDelete ? 1 : 0)
          ^ (this.allowDeleted ? 2 : 0)
          ^ (this.allowDeletedSnapshot ? 4 : 0)
          ^ Objects.hashCode(this.inverseDelete)
          ^ Objects.hashCode(this.objectTypes);
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
