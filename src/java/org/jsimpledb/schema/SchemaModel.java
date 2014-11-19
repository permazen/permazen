
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.util.AbstractXMLStreaming;

/**
 * Models one JSimpleDB {@link org.jsimpledb.core.Database} schema version.
 */
public class SchemaModel extends AbstractXMLStreaming implements XMLConstants, Cloneable {

    static final Map<QName, Class<? extends SchemaField>> FIELD_TAG_MAP = new HashMap<>();
    static {
        FIELD_TAG_MAP.put(COUNTER_FIELD_TAG, CounterSchemaField.class);
        FIELD_TAG_MAP.put(ENUM_FIELD_TAG, EnumSchemaField.class);
        FIELD_TAG_MAP.put(LIST_FIELD_TAG, ListSchemaField.class);
        FIELD_TAG_MAP.put(MAP_FIELD_TAG, MapSchemaField.class);
        FIELD_TAG_MAP.put(REFERENCE_FIELD_TAG, ReferenceSchemaField.class);
        FIELD_TAG_MAP.put(SET_FIELD_TAG, SetSchemaField.class);
        FIELD_TAG_MAP.put(SIMPLE_FIELD_TAG, SimpleSchemaField.class);
    }
    static final Map<QName, Class<? extends SimpleSchemaField>> SIMPLE_FIELD_TAG_MAP = Maps.transformValues(
      Maps.filterValues(SchemaModel.FIELD_TAG_MAP,
      new Predicate<Class<? extends SchemaField>>() {
        @Override
        public boolean apply(Class<? extends SchemaField> type) {
            return SimpleSchemaField.class.isAssignableFrom(type);
        }
      }), new Function<Class<? extends SchemaField>, Class<? extends SimpleSchemaField>>() {
        @Override
        public Class<? extends SimpleSchemaField> apply(Class<? extends SchemaField> type) {
            return type.asSubclass(SimpleSchemaField.class);
        }
      });

    private static final int CURRENT_FORMAT_VERSION = 1;

    private SortedMap<Integer, SchemaObjectType> schemaObjectTypes = new TreeMap<>();

    public SortedMap<Integer, SchemaObjectType> getSchemaObjectTypes() {
        return this.schemaObjectTypes;
    }
    public void setSchemaObjectTypes(SortedMap<Integer, SchemaObjectType> schemaObjectTypes) {
        this.schemaObjectTypes = schemaObjectTypes;
    }

    /**
     * Serialize an instance to the given XML output.
     *
     * @param output XML output
     * @param indent true to pretty print the XML
     * @throws IOException if an I/O error occurs
     */
    public void toXML(OutputStream output, boolean indent) throws IOException {
        try {
            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
            if (indent)
                writer = new IndentXMLStreamWriter(writer);
            writer.writeStartDocument("UTF-8", "1.0");
            this.writeXML(writer);
            writer.writeEndDocument();
            writer.flush();
        } catch (XMLStreamException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            throw new RuntimeException("internal error", e);
        }
        output.flush();
    }

    /**
     * Deserialize an instance from the given XML input and validate it.
     *
     * @param input XML input
     * @throws IOException if an I/O error occurs
     * @throws InvalidSchemaException if the XML input or decoded {@link SchemaModel} is invalid
     */
    public static SchemaModel fromXML(InputStream input) throws IOException {
        final SchemaModel schemaModel = new SchemaModel();
        try {
            final XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
            schemaModel.readXML(reader);
        } catch (XMLStreamException e) {
            throw new InvalidSchemaException("error parsing schema model XML", e);
        }
        schemaModel.validate();
        return schemaModel;
    }

    /**
     * Validate this instance.
     *
     * @throws InvalidSchemaException if this instance is invalid
     */
    public void validate() {
        final TreeMap<String, SchemaObjectType> schemaObjectTypesByName = new TreeMap<>();
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values()) {
            schemaObjectType.validate();
            final String schemaObjectTypeName = schemaObjectType.getName();
            final SchemaObjectType otherSchemaObjectType = schemaObjectTypesByName.put(schemaObjectTypeName, schemaObjectType);
            if (otherSchemaObjectType != null)
                throw new InvalidSchemaException("duplicate object name `" + schemaObjectTypeName + "'");
        }
    }

    /**
     * Determine whether this schema is compatible with the given schema for use with the core API.
     * Two instances are compatible if they are identical in all respects except for object and field names
     * (to also include object and field names in the comparison, use {@link #equals equals()}).
     * The core API uses storage IDs, not names, to identify objects and fields.
     *
     * @param that other schema
     * @throws IllegalArgumentException if {@code that} is null
     */
    public boolean isCompatibleWith(SchemaModel that) {
        if (that == null)
            throw new IllegalArgumentException("null that");
        if (!this.schemaObjectTypes.keySet().equals(that.schemaObjectTypes.keySet()))
            return false;
        for (int storageId : this.schemaObjectTypes.keySet()) {
            final SchemaObjectType thisSchemaObjectType = this.schemaObjectTypes.get(storageId);
            final SchemaObjectType thatSchemaObjectType = that.schemaObjectTypes.get(storageId);
            if (!thisSchemaObjectType.isCompatibleWith(thatSchemaObjectType))
                return false;
        }
        return true;
    }

    void readXML(XMLStreamReader reader) throws XMLStreamException {

        // Read opening tag
        this.expect(reader, false, SCHEMA_MODEL_TAG);

        // Get format version
        final Integer formatAttr = this.getIntAttr(reader, FORMAT_VERSION_ATTRIBUTE, false);
        final int formatVersion = formatAttr != null ? formatAttr : 0;
        final QName objectTypeTag;
        switch (formatVersion) {
        case 0:
            objectTypeTag = new QName("Object");
            break;
        case CURRENT_FORMAT_VERSION:
            objectTypeTag = OBJECT_TYPE_TAG;
            break;
        default:
            throw new XMLStreamException("unrecognized schema format version " + formatAttr, reader.getLocation());
        }

        // Read object type tags
        while (this.expect(reader, true, objectTypeTag)) {
            final SchemaObjectType schemaObjectType = new SchemaObjectType();
            schemaObjectType.readXML(reader, formatVersion);
            final int storageId = schemaObjectType.getStorageId();
            final SchemaObjectType previous = this.schemaObjectTypes.put(storageId, schemaObjectType);
            if (previous != null) {
                throw new XMLStreamException("duplicate use of storage ID " + storageId
                  + " for both " + previous + " and " + schemaObjectType, reader.getLocation());
            }
        }
    }

    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.setDefaultNamespace(SCHEMA_MODEL_TAG.getNamespaceURI());
        writer.writeStartElement(SCHEMA_MODEL_TAG.getNamespaceURI(), SCHEMA_MODEL_TAG.getLocalPart());
        writer.writeAttribute(FORMAT_VERSION_ATTRIBUTE.getNamespaceURI(),
          FORMAT_VERSION_ATTRIBUTE.getLocalPart(), "" + CURRENT_FORMAT_VERSION);
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values())
            schemaObjectType.writeXML(writer);
        writer.writeEndElement();
    }

// Object

    @Override
    public String toString() {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            this.toXML(buf, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new String(buf.toByteArray(), Charset.forName("UTF-8"))
          .replaceAll("(?s)<\\?xml version=\"1\\.0\" encoding=\"UTF-8\"\\?>\n", "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaModel that = (SchemaModel)obj;
        return this.schemaObjectTypes.equals(that.schemaObjectTypes);
    }

    @Override
    public int hashCode() {
        return this.schemaObjectTypes.hashCode();
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    public SchemaModel clone() {
        SchemaModel clone;
        try {
            clone = (SchemaModel)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.schemaObjectTypes = new TreeMap<>();
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values())
            clone.schemaObjectTypes.put(schemaObjectType.getStorageId(), schemaObjectType.clone());
        return clone;
    }
}

