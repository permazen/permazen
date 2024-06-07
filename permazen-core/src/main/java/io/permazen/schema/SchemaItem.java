
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.base.Preconditions;

import io.permazen.core.InvalidSchemaException;
import io.permazen.util.Diffs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Support superclass for schema items that have names.
 */
public abstract class SchemaItem extends SchemaSupport {

    /**
     * The regular expression that all schema item names must match.
     *
     * <p>
     * This pattern is the same as is required for Java identifiers, with the exception that control characters
     * (i.e., {@code 0x0000} through {@code 0x001f}, plus {@code 0x007f}) are disallowed. This restriction ensures
     * that item names are valid in an XML document.
     */
    public static final String NAME_PATTERN = "\\p{javaJavaIdentifierStart}((?!\\p{Cntrl})\\p{javaJavaIdentifierPart})*";

    private String name;
    private int storageId;

// Properties

    /**
     * Get the name associated with this instance, if any.
     *
     * <p>
     * All names must match {@link #NAME_PATTERN}.
     *
     * @return the name of this instance, or null if it has none
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the name associated with this instance.
     *
     * @param name name of this instance
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setName(String name) {
        this.verifyNotLockedDown(false);
        this.name = name;
    }

    /**
     * Get the storage ID associated with this instance.
     *
     * @return the storage ID for this instance, or zero to have one automatically assigned
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Set the storage ID associated with this instance.
     *
     * <p>
     * The default value of zero means one should be automatically assigned when the schema is registered into a database.
     *
     * @param storageId storage ID for this instance, or zero for automatica assignment
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public void setStorageId(int storageId) {
        this.verifyNotLockedDown(true);
        this.storageId = storageId;
    }

// Validation

    void validate() {
        if (name == null)
            throw new InvalidSchemaException(String.format("%s must specify a name", this));
        if (this.storageId < 0)
            throw new InvalidSchemaException(String.format("%s has an invalid storage ID %d", this, this.storageId));
        if (!name.matches(NAME_PATTERN))
            throw new InvalidSchemaException(String.format("%s has an invalid name \"%s\"", this, name));
    }

// Structural Compatibility

    @Override
    final void writeSchemaIdHashData(DataOutputStream output) throws IOException {
        this.writeSchemaIdHashData(output, false);
    }

    // In general, we compute different hashes for SchemaItem's vs. for SchemaModel's
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output);
        if (this.name != null)
            output.writeUTF(this.name);
    }

// DiffGenerating

    protected Diffs differencesFrom(SchemaItem that) {
        Preconditions.checkArgument(that != null, "null that");
        final Diffs diffs = new Diffs();
        if (!Objects.equals(this.name, that.name)) {
            diffs.add(String.format("changed name from %s to %s",
              that.name != null ? "\"" + that.name + "\"" : null,
              this.name != null ? "\"" + this.name + "\"" : null));
        }
        if (this.storageId != that.storageId)
            diffs.add(String.format("changed storage ID from %d to %d", that.storageId, this.storageId));
        return diffs;
    }

// XML Reading

    /**
     * Read in this item's XML.
     *
     * <p>
     * The implementation in {@link SchemaItem} invokes {@link #readAttributes readAttributes()}
     * followed by {@link #readSubElements readSubElements()}.
     *
     * <p>
     * Start state: positioned at opening XML tag.
     * Return state: positioned at closing XML tag.
     */
    void readXML(XMLStreamReader reader, int formatVersion, boolean requireName) throws XMLStreamException {
        this.readAttributes(reader, formatVersion, requireName);
        this.readSubElements(reader, formatVersion);
    }

    void readXML(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.readXML(reader, formatVersion, true);
    }

    /**
     * Read in this item's start tag attributes.
     *
     * <p>
     * The implementation in {@link SchemaItem} reads in an optional name attribute.
     *
     * <p>
     * Start state: positioned at opening XML tag.
     * Return state: same.
     */
    void readAttributes(XMLStreamReader reader, int formatVersion, boolean requireName) throws XMLStreamException {
        final String nameAttr = this.getAttr(reader, XMLConstants.NAME_ATTRIBUTE, requireName);
        if (nameAttr != null)
            this.setName(nameAttr);
        final Integer storageIdAttr = this.getIntAttr(reader, XMLConstants.STORAGE_ID_ATTRIBUTE, false);
        if (storageIdAttr != null)
            this.setStorageId(storageIdAttr);
    }

    /**
     * Read in this item's sub-elements.
     *
     * <p>
     * The implementation in {@link SchemaItem} expects no sub-elements.
     *
     * <p>
     * Start state: positioned at opening XML tag.
     * Return state: positioned at closing XML tag.
     */
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.expectClose(reader);
    }

    /**
     * Read an element found in the given map.
     *
     * @return element found, or null if closing XML tag encountered instead
     */
    <T> T readMappedType(XMLStreamReader reader, boolean closingOK, Map<QName, Class<? extends T>> tagMap)
      throws XMLStreamException {

        // Expect to see one of the map's XML tag keys
        if (!this.expect(reader, closingOK, tagMap.keySet().toArray(new QName[tagMap.size()])))
            return null;

        // Instantiate the corresponding type
        T obj = null;
        for (Map.Entry<QName, Class<? extends T>> entry : tagMap.entrySet()) {
            if (reader.getName().equals(entry.getKey())) {
                try {
                    return entry.getValue().getConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("unexpected exception", e);
                }
            }
        }
        throw new RuntimeException("internal error: didn't find " + reader.getName());
    }

    /**
     * Read an {@link Enum} attribute.
     *
     * @param reader XML reader
     * @param type {@link Enum} type
     * @param name attribute name
     * @param defaultValue default value, or null if value is required
     */
    <T extends Enum<T>> T readAttr(XMLStreamReader reader, Class<T> type, QName name, T defaultValue) throws XMLStreamException {
        final String text = this.getAttr(reader, name, defaultValue == null);
        if (text == null)
            return defaultValue;
        try {
            return Enum.valueOf(type, text);
        } catch (IllegalArgumentException e) {
            throw this.newInvalidInputException(reader, e,
              "invalid value \"%s\" for \"%s\" attribute in %s",
              text, name.getLocalPart(), this);
        }
    }

// XML Writing

    abstract void writeXML(XMLStreamWriter writer, boolean includeStorageIds, boolean prettyPrint) throws XMLStreamException;

    void writeStartItemElement(XMLStreamWriter writer) throws XMLStreamException {
        this.writeStartElement(writer, this.getItemType().getElementName());
    }

    void writeEmptyItemElement(XMLStreamWriter writer) throws XMLStreamException {
        this.writeEmptyElement(writer, this.getItemType().getElementName());
    }

    final void writeAttributes(XMLStreamWriter writer, boolean includeStorageIds) throws XMLStreamException {
        this.writeAttributes(writer, includeStorageIds, true);
    }

    void writeAttributes(XMLStreamWriter writer, boolean includeStorageIds, boolean includeName) throws XMLStreamException {
        if (includeStorageIds && this.storageId != 0)
            this.writeAttr(writer, XMLConstants.STORAGE_ID_ATTRIBUTE, this.storageId);
        if (includeName && this.name != null)
            this.writeAttr(writer, XMLConstants.NAME_ATTRIBUTE, this.name);
    }

// Object

    @Override
    public abstract String toString();

    String toStringName() {
        return this.name != null ? "\"" + this.name + "\"" : "(anonymous)";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaItem that = (SchemaItem)obj;
        return Objects.equals(this.name, that.name)
          && this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ Objects.hashCode(this.name)
          ^ this.storageId;
    }

// Cloneable

    @Override
    protected SchemaItem clone() {
        return (SchemaItem)super.clone();
    }
}
