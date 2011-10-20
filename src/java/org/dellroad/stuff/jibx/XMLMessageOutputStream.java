
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.jibx.runtime.JiBXException;

/**
 * {@link OutputStream} over which XML documents are passed. This class is a companion to {@link XMLMessageOutputStream}.
 *
 * <p>
 * XML documents are created from Java objects via {@link JiBXUtil#writeObject(Object, OutputStream) JiBXUtil.writeObject()}.
 * </p>
 *
 * @param <T> XML document type
 * @see XMLMessageInputStream
 */
public class XMLMessageOutputStream<T> {

    private final Class<T> type;
    private final BufferedOutputStream output;

    /**
     * Constructor.
     *
     * @param type Java type for XML documents
     * @param output data destination
     */
    public XMLMessageOutputStream(Class<T> type, OutputStream output) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (output == null)
            throw new IllegalArgumentException("null output");
        this.type = type;
        this.output = new BufferedOutputStream(output);
    }

    /**
     * Write the object encoded as XML to the underlying output stream.
     * The underlying output stream is flushed.
     */
    public void writeDocument(T obj) throws IOException, JiBXException {
        JiBXUtil.writeObject(obj, this.output);
        this.output.flush();
    }

    /**
     * Close the underlying output stream.
     */
    public void close() throws IOException {
        this.output.close();
    }
}

