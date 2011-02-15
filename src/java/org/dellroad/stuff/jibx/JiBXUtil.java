
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.jibx.runtime.JiBXException;
import org.springframework.oxm.jibx.JibxMarshaller;

/**
 * Some simplified API methods for JiBX XML encoding/decoding.
 */
public final class JiBXUtil {

    private JiBXUtil() {
    }

    /**
     * Read in an object encoded as XML.
     *
     * <p>
     * The {@code input} is not closed by this method.
     *
     * @param input source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading from {@code input}
     */
    @SuppressWarnings("unchecked")
    public static <T> T readObject(Class<T> clazz, InputStream input) throws JiBXException, IOException {

        // Set up JiBX
        JibxMarshaller unmarshaller = new JibxMarshaller();
        unmarshaller.setTargetClass(clazz);
        unmarshaller.afterPropertiesSet();

        // Read and parse XML
        return (T)unmarshaller.unmarshal(new StreamSource(input));
    }

    /**
     * Read in an object encoded as XML from an {@link URL}.
     *
     * @param url source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading the referenced document
     */
    public static <T> T readObject(Class<T> clazz, URL url) throws JiBXException, IOException {
        InputStream in = url.openStream();
        try {
            return JiBXUtil.readObject(clazz, in);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Write out the given instance encoded as a UTF-8 encoded XML document.
     *
     * @param output output destination; will not be closed by this method
     * @throws JiBXException if there is a JiBX encoding error
     * @throws IOException if an error occurs writing to {@code output}
     */
    public static <T> void writeObject(T obj, OutputStream output) throws JiBXException, IOException {
        JiBXUtil.writeObject(obj, new OutputStreamWriter(output, "UTF-8"));
    }

    /**
     * Write out the given instance encoded as an XML document with "UTF-8" as the declared encoding.
     *
     * @param output output destination; will be closed by this method if successful
     */
    public static <T> void writeObject(T obj, Writer output) throws JiBXException, IOException {

        // Set up JiBX
        JibxMarshaller marshaller = new JibxMarshaller();
        marshaller.setTargetClass(obj.getClass());
        marshaller.setEncoding("UTF-8");
        marshaller.setIndent(4);
        marshaller.afterPropertiesSet();

        // Output XML
        BufferedWriter bufferedWriter = new BufferedWriter(output);
        marshaller.marshal(obj, new StreamResult(bufferedWriter));
    }

    /**
     * Encode the given instance as an XML document and return it as a {@link String}.
     *
     * @throws JiBXException if there is a JiBX encoding error
     */
    public static <T> String toString(T obj) throws IOException, JiBXException {
        final StringWriter w = new StringWriter();
        JiBXUtil.writeObject(obj, w);
        return w.toString();
    }
}

