
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
import java.util.concurrent.Callable;

import org.dellroad.stuff.java.IdGenerator;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
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
     * This method assumes there is exactly one binding for the given class.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * <p>
     * The {@code input} is not closed by this method.
     *
     * @param targetClass target class
     * @param input source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading from {@code input}
     */
    public static <T> T readObject(Class<T> targetClass, InputStream input) throws JiBXException, IOException {
        return JiBXUtil.readObject(targetClass, null, input);
    }

    /**
     * Read in an object encoded as XML.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * <p>
     * The {@code input} is not closed by this method.
     *
     * @param targetClass target class
     * @param bindingName binding name, or null to choose the only one
     * @param input source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading from {@code input}
     */
    public static <T> T readObject(final Class<T> targetClass, String bindingName, final InputStream input)
      throws JiBXException, IOException {
        IBindingFactory bindingFactory = bindingName != null ?
          BindingDirectory.getFactory(bindingName, targetClass) : BindingDirectory.getFactory(targetClass);
        final IUnmarshallingContext unmarshallingContext = bindingFactory.createUnmarshallingContext();
        try {
            return IdGenerator.run(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return targetClass.cast(unmarshallingContext.unmarshalDocument(input, null));
                }
            });
        } catch (Exception e) {
            JiBXUtil.unwrapException(e);
            return null;            // not reached
        }
    }

    /**
     * Read in an object encoded as XML from an {@link URL}.
     * This method assumes there is exactly one binding for the given class.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param targetClass target class
     * @param url source for the XML document
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading the referenced document
     */
    public static <T> T readObject(Class<T> targetClass, URL url) throws JiBXException, IOException {
        return JiBXUtil.readObject(targetClass, null, url);
    }

    /**
     * Read in an object encoded as XML from an {@link URL}.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param targetClass target class
     * @param url source for the XML document
     * @param bindingName binding name, or null to choose the only one
     * @throws JiBXException if there is a JiBX parse error
     * @throws IOException if an error occurs reading the referenced document
     */
    public static <T> T readObject(Class<T> targetClass, String bindingName, URL url) throws JiBXException, IOException {
        InputStream in = url.openStream();
        try {
            return JiBXUtil.readObject(targetClass, bindingName, in);
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
     * This method assumes there is exactly one binding for the given class.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to write
     * @param output output destination; will <b>not</b> be closed by this method
     * @throws JiBXException if there is a JiBX encoding error
     * @throws IOException if an error occurs writing to {@code output}
     */
    public static <T> void writeObject(T obj, OutputStream output) throws JiBXException, IOException {
        JiBXUtil.writeObject(obj, null, output);
    }

    /**
     * Write out the given instance encoded as a UTF-8 encoded XML document.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to write
     * @param bindingName binding name, or null to choose the only one
     * @param output output destination; will <b>not</b> be closed by this method
     * @throws JiBXException if there is a JiBX encoding error
     * @throws IOException if an error occurs writing to {@code output}
     */
    public static <T> void writeObject(T obj, String bindingName, OutputStream output) throws JiBXException, IOException {
        JiBXUtil.writeObject(obj, bindingName, new OutputStreamWriter(output, XML_ENCODING));
    }

    /**
     * Write out the given instance encoded as an XML document with "UTF-8" as the declared encoding.
     * This method assumes there is exactly one binding for the given class.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to write
     * @param writer output destination; will <b>not</b> be closed by this method
     */
    public static <T> void writeObject(T obj, Writer writer) throws JiBXException, IOException {
        JiBXUtil.writeObject(obj, null, writer);
    }

    /**
     * Write out the given instance encoded as an XML document with "UTF-8" as the declared encoding.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to write
     * @param bindingName binding name, or null to choose the only one
     * @param writer output destination; will <b>not</b> be closed by this method
     */
    public static void writeObject(final Object obj, String bindingName, final Writer writer) throws JiBXException, IOException {
        IBindingFactory bindingFactory = bindingName != null ?
          BindingDirectory.getFactory(bindingName, obj.getClass()) : BindingDirectory.getFactory(obj.getClass());
        final IMarshallingContext marshallingContext = bindingFactory.createMarshallingContext();
        try {
            IdGenerator.run(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    BufferedWriter bufferedWriter = new BufferedWriter(writer);
                    marshallingContext.setIndent(4);
                    marshallingContext.setOutput(bufferedWriter);
                    marshallingContext.startDocument(XML_ENCODING, null);
                    ((IMarshallable)obj).marshal(marshallingContext);
                    marshallingContext.getXmlWriter().flush();
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    return null;
                }
            });
        } catch (Exception e) {
            JiBXUtil.unwrapException(e);
        }
    }

    /**
     * Encode the given instance as an XML document and return it as a {@link String}.
     * This method assumes there is exactly one binding for the given class.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to encode
     * @throws JiBXException if there is a JiBX encoding error
     */
    public static String toString(Object obj) throws JiBXException {
        return JiBXUtil.toString(obj, null);
    }

    /**
     * Encode the given instance as an XML document and return it as a {@link String}.
     *
     * <p>
     * This method runs within a new invocation of {@link IdGenerator#run(Callable) IdGenerator.run()} to support object references
     * (see {@link IdMapper}).
     *
     * @param obj object to encode
     * @param bindingName binding name, or null to choose the only one
     * @throws JiBXException if there is a JiBX encoding error
     */
    public static String toString(Object obj, String bindingName) throws JiBXException {
        final StringWriter w = new StringWriter();
        try {
            JiBXUtil.writeObject(obj, bindingName, w);
            w.close();
        } catch (IOException e) {
            throw new JiBXException("unexpected exception", e);
        }
        return w.toString();
    }

    private static void unwrapException(Exception e) throws JiBXException, IOException {
        if (e instanceof JiBXException)
            throw (JiBXException)e;
        if (e instanceof IOException)
            throw (IOException)e;
        if (e instanceof RuntimeException)
            throw (RuntimeException)e;
        throw new RuntimeException(e);
    }
}

