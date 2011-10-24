
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.dellroad.stuff.io.InputStreamReader;
import org.jibx.runtime.JiBXException;

/**
 * {@link InputStream} over which XML documents are passed. This class is a companion to {@link XMLDocumentOutputStream}.
 *
 * <p>
 * XML documents are converted into Java objects via {@link JiBXUtil#readObject(Class, InputStream) JiBXUtil.readObject()}.
 * </p>
 *
 * <p>
 * Instances of this class are thread-safe.
 * </p>
 *
 * @param <T> XML document type
 * @see XMLDocumentOutputStream
 */
public class XMLDocumentInputStream<T> {

    private final Class<T> type;
    private final InputStreamReader input;

    /**
     * Constructor.
     *
     * @param type Java type for XML documents
     * @param input data source
     */
    public XMLDocumentInputStream(Class<T> type, InputStream input) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (input == null)
            throw new IllegalArgumentException("null input");
        this.type = type;
        this.input = new InputStreamReader(new BufferedInputStream(input));
    }

    /**
     * Read the next XML document, parsed and objectified.
     *
     * @return decoded object or {@code null} on EOF
     */
    public T read() throws IOException, JiBXException {
        InputStream xml = this.input.read();
        if (xml == null)
            return null;
        try {
            return JiBXUtil.readObject(this.type, xml);
        } finally {
            try {
                xml.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public void close() throws IOException {
        this.input.close();
    }
}

