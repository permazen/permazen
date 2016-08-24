
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.util.AbstractXMLStreaming;
import org.jsimpledb.util.Diffs;

/**
 * Common superclass for {@link SchemaObjectType} and {@link SchemaField}.
 */
public abstract class AbstractSchemaItem extends AbstractXMLStreaming implements XMLConstants, Cloneable {

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
        this.storageId = storageId;
    }

    void validate() {
        if (name == null)
            throw new InvalidSchemaException(this + " must specify a name");
        if (!name.matches(SchemaItem.NAME_PATTERN))
            throw new InvalidSchemaException(this + " has an invalid name `" + name + "'");
        if (this.storageId <= 0)
            throw new InvalidSchemaException(this + " has an invalid storage ID " + this.storageId + "; must be greater than zero");
    }

    /**
     * Determine whether this instance is compatible with the given instance for use with the core API.
     * Two instances are compatible if they are identical in all respects except for object and field names
     * (to also include object and field names in the comparison, use {@link #equals equals()}).
     * The core API uses storage IDs, not names, to identify objects and fields.
     *
     * @param that other schema object
     * @return true if this and {@code that} are compatible
     * @throws IllegalArgumentException if {@code that} is null
     */
    public final boolean isCompatibleWith(AbstractSchemaItem that) {
        Preconditions.checkArgument(that != null, "null that");
        if (this.storageId != that.storageId)
            return false;
        if (this.getClass() != that.getClass())
            return false;
        return this.isCompatibleWithInternal(that);
    }

    abstract boolean isCompatibleWithInternal(AbstractSchemaItem that);

    static <K> boolean allAreCompatible(Map<K, ? extends AbstractSchemaItem> map1, Map<K, ? extends AbstractSchemaItem> map2) {
        if (!map1.keySet().equals(map2.keySet()))
            return false;
        for (Map.Entry<K, ? extends AbstractSchemaItem> entry : map1.entrySet()) {
            final K key = entry.getKey();
            final AbstractSchemaItem item1 = entry.getValue();
            final AbstractSchemaItem item2 = map2.get(key);
            if (!item1.isCompatibleWith(item2))
                return false;
        }
        return true;
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
        final Integer storageIdAttr = this.getIntAttr(reader, STORAGE_ID_ATTRIBUTE, false);
        if (storageIdAttr != null)
            this.setStorageId(storageIdAttr);
        final String nameAttr = this.getAttr(reader, NAME_ATTRIBUTE, false);
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

// XML Writing

    abstract void writeXML(XMLStreamWriter writer) throws XMLStreamException;

    final void writeAttributes(XMLStreamWriter writer) throws XMLStreamException {
        this.writeAttributes(writer, true);
    }

    void writeAttributes(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeAttribute(STORAGE_ID_ATTRIBUTE.getNamespaceURI(), STORAGE_ID_ATTRIBUTE.getLocalPart(), "" + this.storageId);
        if (includeName && this.name != null)
            writer.writeAttribute(NAME_ATTRIBUTE.getNamespaceURI(), NAME_ATTRIBUTE.getLocalPart(), this.name);
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
        return (this.name != null ? this.name.equals(that.name) : that.name == null) && this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return (this.name != null ? this.name.hashCode() : 0) ^ this.storageId;
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    public AbstractSchemaItem clone() {
        try {
            return (AbstractSchemaItem)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

// NameComparator

    static class NameComparator implements Comparator<AbstractSchemaItem>, Serializable {

        private static final long serialVersionUID = 3020319677602098674L;

        @Override
        public int compare(AbstractSchemaItem item1, AbstractSchemaItem item2) {
            return item1.getName().compareTo(item2.getName());
        }
    }
}

