
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
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dellroad.stuff.jibx.JiBXUtil;
import org.jibx.runtime.JiBXException;
import org.jsimpledb.core.InvalidSchemaException;

/**
 * Models one JSimpleDB {@link org.jsimpledb.core.Database} schema version.
 */
public class SchemaModel implements Cloneable {

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
            JiBXUtil.writeObject(this, output);
        } catch (JiBXException e) {
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
        final SchemaModel schemaModel;
        try {
            schemaModel = JiBXUtil.readObject(SchemaModel.class, input);
        } catch (JiBXException e) {
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
            clone.addSchemaObject(schemaObject.clone());
        return clone;
    }

// JiBX

    public void addSchemaObject(SchemaObject type) {
        this.schemaObjects.put(type.getStorageId(), type);
    }

    public Iterator<SchemaObject> iterateSchemaObjects() {
        return this.schemaObjects.values().iterator();
    }
}

