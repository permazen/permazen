
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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.dellroad.stuff.jibx.JiBXUtil;
import org.jibx.runtime.JiBXException;

/**
 * Models one {@link org.jsimpledb.JSimpleDB} schema version.
 */
public class SchemaModel implements Cloneable {

    private SortedMap<Integer, SchemaObject> schemaObjects = new TreeMap<>();

    @NotNull
    @Valid
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
     * Deserialize an instance from the given XML input.
     *
     * @param input XML input
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the XML input is invalid
     */
    public static SchemaModel fromXML(InputStream input) throws IOException {
        try {
            return JiBXUtil.readObject(SchemaModel.class, input);
        } catch (JiBXException e) {
            throw new IllegalArgumentException("invalid XML", e);
        }
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

