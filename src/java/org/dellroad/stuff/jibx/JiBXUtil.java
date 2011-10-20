
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

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IMarshallable;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 * Some simplified API methods for JiBX XML encoding/decoding.
 */
public final class JiBXUtil {

    public static final String XML_ENCODING = "UTF-8";

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
    public static <T> T readObject(Class<T> targetClass, InputStream input) throws JiBXException, IOException {
        IUnmarshallingContext unmarshallingContext = BindingDirectory.getFactory(targetClass).createUnmarshallingContext();
        return targetClass.cast(unmarshallingContext.unmarshalDocument(input, null));
    }

    /**
     * Read in an object encoded as XML from an {@link URL}.
     *
     * @param url source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading the referenced document
     */
    public static <T> T readObject(Class<T> targetClass, URL url) throws JiBXException, IOException {
        InputStream in = url.openStream();
        try {
            return JiBXUtil.readObject(targetClass, in);
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
     * @param output output destination; will <b>not</b> be closed by this method
     * @throws JiBXException if there is a JiBX encoding error
     * @throws IOException if an error occurs writing to {@code output}
     */
    public static <T> void writeObject(T obj, OutputStream output) throws JiBXException, IOException {
        JiBXUtil.writeObject(obj, new OutputStreamWriter(output, XML_ENCODING));
    }

    /**
     * Write out the given instance encoded as an XML document with "UTF-8" as the declared encoding.
     *
     * @param writer output destination; will <b>not</b> be closed by this method
     */
    public static <T> void writeObject(T obj, Writer writer) throws JiBXException, IOException {
        IMarshallingContext marshallingContext = BindingDirectory.getFactory(obj.getClass()).createMarshallingContext();
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        marshallingContext.setIndent(4);
        marshallingContext.setOutput(bufferedWriter);
        marshallingContext.startDocument(XML_ENCODING, null);
        ((IMarshallable)obj).marshal(marshallingContext);
        marshallingContext.getXmlWriter().flush();
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    /**
     * Encode the given instance as an XML document and return it as a {@link String}.
     *
     * @throws JiBXException if there is a JiBX encoding error
     */
    public static <T> String toString(T obj) throws IOException, JiBXException {
        final StringWriter w = new StringWriter();
        JiBXUtil.writeObject(obj, w);
        w.close();
        return w.toString();
    }
}

