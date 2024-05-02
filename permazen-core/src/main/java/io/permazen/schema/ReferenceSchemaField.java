
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

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.REFERENCE_FIELD;

    private DeleteAction inverseDelete;
    private boolean forwardDelete;
    private boolean allowDeleted;
    private NavigableSet<String> objectTypes;

// Properties

    /**
     * Get the desired behavior when an object referred to by this field is deleted.
     *
     * @return desired behavior when a referenced object is deleted
     */
    public DeleteAction getInverseDelete() {
        return this.inverseDelete;
    }

    /**
     * Set the desired behavior when an object referred to by this field is deleted.
     *
     * @param inverseDelete action on deletion of target object
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setInverseDelete(DeleteAction inverseDelete) {
        this.verifyNotLockedDown(false);
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

    /**
     * Set the whether to forward cascade delete operations.
     *
     * @param forwardDelete true to forward cascade delete operations, false to do nothing
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setForwardDelete(boolean forwardDelete) {
        this.verifyNotLockedDown(false);
        this.forwardDelete = forwardDelete;
    }

    /**
     * Determine whether this field accepts references to deleted objects in normal (non-detached) transactions.
     *
     * @return whether deleted objects are allowed in normal transactions
     */
    public boolean isAllowDeleted() {
        return this.allowDeleted;
    }

    /**
     * Set the whether this field may reference non-existent objects in normal (non-detached) transactions.
     *
     * @param allowDeleted true to allow dangling references, otherwise false
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setAllowDeleted(boolean allowDeleted) {
        this.verifyNotLockedDown(false);
        this.allowDeleted = allowDeleted;
    }

    /**
     * Get the object types this field is allowed to reference, if so restricted.
     *
     * <p>
     * If not null, the returned set will be unmodifiable if this instance is locked down.
     *
     * @return names allowed object types, or null if there is no restriction
     */
    public NavigableSet<String> getObjectTypes() {
        return this.objectTypes;
    }

    /**
     * Set the object types this field is allowed to reference.
     *
     * @param objectTypes names of the allowed object types, or null if there is no restriction
     */
    public void setObjectTypes(NavigableSet<String> objectTypes) {
        this.verifyNotLockedDown(false);
        this.objectTypes = objectTypes != null ? new TreeSet<>(objectTypes) : null;
    }

    @Override
    public boolean hasFixedEncoding() {
        return true;
    }

    @Override
    public boolean isAlwaysIndexed() {
        return true;
    }

// Lockdown

    @Override
    void lockDown1() {
        super.lockDown1();
        if (this.objectTypes != null)
            this.objectTypes = Collections.unmodifiableNavigableSet(this.objectTypes);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.inverseDelete == null)
            throw new InvalidSchemaException(String.format("invalid %s: no inverse delete action specified", this));
        switch (this.inverseDelete) {
        case IGNORE:
            if (!this.allowDeleted) {
                throw new InvalidSchemaException(String.format(
                  "invalid %s: inverse delete %s is incompatible with disallowing dangling references", this, this.inverseDelete));
            }
            break;
        case REMOVE:
            if (this.getParent() == null) {
                throw new InvalidSchemaException(String.format(
                  "invalid %s: inverse delete %s is only appropriate for complex sub-fields", this, this.inverseDelete));
            }
            break;
        default:
            break;
        }
        if (this.objectTypes != null && this.objectTypes.stream().anyMatch(Objects::isNull))
            throw new InvalidSchemaException(String.format("invalid %s: object types contains null", this));
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseReferenceSchemaField(this);
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        output.writeBoolean(forSchemaModel);
        if (forSchemaModel) {
            output.writeUTF(this.inverseDelete.name());
            output.writeBoolean(this.forwardDelete);
            output.writeBoolean(this.allowDeleted);
            output.writeBoolean(this.objectTypes != null);
            if (this.objectTypes != null) {
                output.writeInt(this.objectTypes.size());
                for (String typeName : this.objectTypes)
                    output.writeUTF(typeName);
            }
        }
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof ReferenceSchemaField)) {
            diffs.add(String.format("changed %s from %s to %s",
              "type", other.getClass().getSimpleName(), this.getClass().getSimpleName()));
            return diffs;
        }
        final ReferenceSchemaField that = (ReferenceSchemaField)other;
        if (!Objects.equals(this.inverseDelete, that.inverseDelete))
            diffs.add(String.format("changed %s from %s to %s", "inverse delete", that.inverseDelete, this.inverseDelete));
        if (this.forwardDelete != that.forwardDelete)
            diffs.add(String.format("changed %s from %s to %s", "forward delete", that.forwardDelete, this.forwardDelete));
        if (this.allowDeleted != that.allowDeleted)
            diffs.add(String.format("changed %s from %s to %s", "allow deleted objects", that.allowDeleted, this.allowDeleted));
        if (!Objects.equals(this.objectTypes, that.objectTypes))
            diffs.add(String.format("changed %s from %s to %s", "object types", that.objectTypes, this.objectTypes));
        return diffs;
    }

// XML Reading

    @Override
    void readAttributes(XMLStreamReader reader, int formatVersion, boolean requireName) throws XMLStreamException {
        super.readAttributes(reader, formatVersion, requireName);
        this.setInverseDelete(this.readAttr(reader, DeleteAction.class,
          XMLConstants.INVERSE_DELETE_ATTRIBUTE, DeleteAction.EXCEPTION));
        final Boolean forwardDeleteAttr = this.getBooleanAttr(reader, XMLConstants.FORWARD_DELETE_ATTRIBUTE, false);
        if (forwardDeleteAttr != null)
            this.setForwardDelete(forwardDeleteAttr);
        this.setAllowDeleted(this.getInverseDelete() == DeleteAction.IGNORE);   // defaults to false unless DeleteAction.IGNORE
        final Boolean allowDeletedAttr = this.getBooleanAttr(reader, XMLConstants.ALLOW_DELETED_ATTRIBUTE, false);
        if (allowDeletedAttr != null)
            this.setAllowDeleted(allowDeletedAttr);
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {

        // Any restrictions?
        if (!this.expect(reader, true, XMLConstants.OBJECT_TYPES_TAG)) {
            this.objectTypes = null;
            return;
        }

        // Read list of zero or more permitted type names
        this.objectTypes = new TreeSet<>();
        while (this.expect(reader, true, XMLConstants.OBJECT_TYPE_TAG)) {
            this.objectTypes.add(this.getAttr(reader, XMLConstants.NAME_ATTRIBUTE));
            this.expectClose(reader);           // </ObjectType>
        }

        // Read closing </ReferenceField>
        this.expectClose(reader);
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean prettyPrint, boolean includeName) throws XMLStreamException {
        if (this.objectTypes != null)
            this.writeStartElement(writer, XMLConstants.REFERENCE_FIELD_TAG);
        else
            this.writeEmptyElement(writer, XMLConstants.REFERENCE_FIELD_TAG);
        this.writeAttributes(writer, includeName);
        if (this.inverseDelete != null)
            this.writeAttr(writer, XMLConstants.INVERSE_DELETE_ATTRIBUTE, this.inverseDelete.name());
        if (this.allowDeleted != (this.inverseDelete == DeleteAction.IGNORE))
            this.writeAttr(writer, XMLConstants.ALLOW_DELETED_ATTRIBUTE, this.allowDeleted);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
        if (this.objectTypes != null) {
            this.writeStartElement(writer, XMLConstants.OBJECT_TYPES_TAG);
            for (String typeName : this.objectTypes) {
                this.writeEmptyElement(writer, XMLConstants.OBJECT_TYPE_TAG);
                this.writeAttr(writer, XMLConstants.NAME_ATTRIBUTE, typeName);
            }
            writer.writeEndElement();           // </ObjectTypes>
            writer.writeEndElement();           // </ReferenceField>
        }
    }

    @Override
    void writeSimpleAttributes(XMLStreamWriter writer) throws XMLStreamException {
        super.writeSimpleAttributes(writer);
        if (this.forwardDelete)
            this.writeAttr(writer, XMLConstants.FORWARD_DELETE_ATTRIBUTE, this.forwardDelete);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ReferenceSchemaField that = (ReferenceSchemaField)obj;
        return Objects.equals(this.inverseDelete, that.inverseDelete)
          && this.forwardDelete == that.forwardDelete
          && this.allowDeleted == that.allowDeleted
          && Objects.equals(this.objectTypes, that.objectTypes);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ Objects.hashCode(this.inverseDelete)
          ^ (this.forwardDelete ? 2 : 0)
          ^ (this.allowDeleted ? 4 : 0)
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
