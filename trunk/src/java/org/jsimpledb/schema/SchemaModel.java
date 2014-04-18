
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.SortedMap;
import java.util.TreeMap;

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

    private SortedMap<Integer, SchemaObject> schemaObjects = new TreeMap<>();

    public SortedMap<Integer, SchemaObject> getSchemaObjects() {
        return this.schemaObjects;
    }
    public void setSchemaObjects(SortedMap<Integer, SchemaObject> schemaObjects) {
        this.schemaObjects = schemaObjects;
    }

    /**
     * Serialize an instance to the given XML output.
     *
     * @param output XML output
     * @throws IOException if an I/O error occurs
     */
    public void toXML(OutputStream output) throws IOException {
        try {
            final XMLStreamWriter writer = new IndentXMLStreamWriter(
              XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
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
        for (SchemaObject schemaObject : this.schemaObjects.values())
            schemaObject.validate();
    }

    void readXML(XMLStreamReader reader) throws XMLStreamException {
        this.expect(reader, false, SCHEMA_MODEL_TAG);
        while (this.expect(reader, true, OBJECT_TAG)) {
            final SchemaObject schemaObject = new SchemaObject();
            schemaObject.readXML(reader);
            final int storageId = schemaObject.getStorageId();
            final SchemaObject previous = this.schemaObjects.put(storageId, schemaObject);
            if (previous != null) {
                throw new InvalidSchemaException("duplicate use of storage ID " + storageId
                  + " for both " + previous + " and " + schemaObject);
            }
        }
    }

    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.setDefaultNamespace(SCHEMA_MODEL_TAG.getNamespaceURI());
        writer.writeStartElement(SCHEMA_MODEL_TAG.getNamespaceURI(), SCHEMA_MODEL_TAG.getLocalPart());
        for (SchemaObject schemaObject : this.schemaObjects.values())
            schemaObject.writeXML(writer);
        writer.writeEndElement();
    }

// Object

    @Override
    public String toString() {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            this.toXML(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new String(buf.toByteArray(), Charset.forName("UTF-8"));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaModel that = (SchemaModel)obj;
        return this.schemaObjects.equals(that.schemaObjects);
    }

    @Override
    public int hashCode() {
        return this.schemaObjects.hashCode();
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
        clone.schemaObjects = new TreeMap<>();
        for (SchemaObject schemaObject : this.schemaObjects.values())
            clone.schemaObjects.put(schemaObject.getStorageId(), schemaObject.clone());
        return clone;
    }
}

