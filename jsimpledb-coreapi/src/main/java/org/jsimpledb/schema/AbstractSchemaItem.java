
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import com.google.common.base.Preconditions;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.util.Diffs;

/**
 * Common superclass for {@link SchemaObjectType} and {@link SchemaField}.
 */
public abstract class AbstractSchemaItem extends SchemaSupport {

    private String name;
    private int storageId;

    /**
     * Get the name associated with this instance, if any.
     *
     * @return the name of this instance, or null if it has none
     */
    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.verifyNotLockedDown();
        this.name = name;
    }

    /**
     * Get the storage ID associated with this instance.
     * Storage IDs must be positive values.
     *
     * @return the storage ID for this instance
     */
    public int getStorageId() {
        return this.storageId;
    }
    public void setStorageId(int storageId) {
        this.verifyNotLockedDown();
        this.storageId = storageId;
    }

// Validation

    void validate() {
        if (name == null)
            throw new InvalidSchemaException(this + " must specify a name");
        if (!name.matches(SchemaItem.NAME_PATTERN))
            throw new InvalidSchemaException(this + " has an invalid name `" + name + "'");
        if (this.storageId <= 0)
            throw new InvalidSchemaException(this + " has an invalid storage ID " + this.storageId + "; must be greater than zero");
    }

// Compatibility

    static <K, V> boolean isAll(Map<K, V> map1, Map<K, V> map2, BiPredicate<V, V> checker) {
        if (!map1.keySet().equals(map2.keySet()))
            return false;
        for (Map.Entry<K, V> entry : map1.entrySet()) {
            final K key = entry.getKey();
            final V value1 = entry.getValue();
            final V value2 = map2.get(key);
            if (value1 == null || value2 == null || !checker.test(value1, value2))
                return false;
        }
        return true;
    }

    void writeCompatibilityHashData(DataOutputStream output) throws IOException {
        output.writeUTF(this.getClass().getSimpleName());
        output.writeInt(this.storageId);
    }

// DiffGenerating

    protected Diffs differencesFrom(AbstractSchemaItem that) {
        Preconditions.checkArgument(that != null, "null that");
        final Diffs diffs = new Diffs();
        if (!(this.name != null ? this.name.equals(that.name) : that.name == null)) {
            diffs.add("changed name from " + (that.name != null ? "`" + that.name + "'" : null)
              + " to " + (this.name != null ? "`" + this.name + "'" : null));
        }
        if (this.storageId != that.storageId)
            diffs.add("changed storage ID from " + that.storageId + " to " + this.storageId);
        return diffs;
    }

// XML Reading

    /**
     * Read in this item's XML.
     *
     * <p>
     * The implementation in {@link AbstractSchemaItem} invokes {@link #readAttributes readAttributes()}
     * followed by {@link #readSubElements readSubElements()}.
     *
     * <p>
     * Start state: positioned at opening XML tag.
     * Return state: positioned at closing XML tag.
     */
    void readXML(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        this.readAttributes(reader, formatVersion);
        this.readSubElements(reader, formatVersion);
    }

    /**
     * Read in this item's start tag attributes.
     *
     * <p>
     * The implementation in {@link AbstractSchemaItem} reads in required storage ID and name attributes.
     *
     * <p>
     * Start state: positioned at opening XML tag.
     * Return state: same.
     */
    void readAttributes(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        final Integer storageIdAttr = this.getIntAttr(reader, XMLConstants.STORAGE_ID_ATTRIBUTE, false);
        if (storageIdAttr != null)
            this.setStorageId(storageIdAttr);
        final String nameAttr = this.getAttr(reader, XMLConstants.NAME_ATTRIBUTE, false);
        if (nameAttr != null)
            this.setName(nameAttr);
    }

    /**
     * Read in this item's sub-elements.
     *
     * <p>
     * The implementation in {@link AbstractSchemaItem} expects no sub-elements.
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
                    return entry.getValue().newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException("unexpected exception", e);
                }
            }
        }
        throw new RuntimeException("internal error: didn't find " + reader.getName() + " in tagMap");
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
            throw new XMLStreamException("invalid value `" + text
              + " for \"" + name.getLocalPart() + "\" attribute in " + this, reader.getLocation());
        }
    }

// XML Writing

    abstract void writeXML(XMLStreamWriter writer) throws XMLStreamException;

    final void writeAttributes(XMLStreamWriter writer) throws XMLStreamException {
        this.writeAttributes(writer, true);
    }

    void writeAttributes(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeAttribute(XMLConstants.STORAGE_ID_ATTRIBUTE.getNamespaceURI(),
          XMLConstants.STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + this.storageId);
        if (includeName && this.name != null) {
            writer.writeAttribute(XMLConstants.NAME_ATTRIBUTE.getNamespaceURI(),
              XMLConstants.NAME_ATTRIBUTE.getLocalPart(), this.name);
        }
    }

// Object

    @Override
    public String toString() {
        return "#" + this.storageId + (this.name != null ? " `" + this.name + "'" : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractSchemaItem that = (AbstractSchemaItem)obj;
        return Objects.equals(this.name, that.name) && this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.name) ^ this.storageId;
    }

// Cloneable

    @Override
    protected AbstractSchemaItem clone() {
        return (AbstractSchemaItem)super.clone();
    }
}

